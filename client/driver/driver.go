package client

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/WatchJani/memCashed/client/internal/types"
	p "github.com/WatchJani/memCashed/client/parser"
)

const (
	defaultQueueSize      = 65536
	defaultTimeout        = 5 * time.Second
	defaultReadBufferSize = 1024*1024 + 64
	defaultReconnectDelay = 100 * time.Millisecond
	defaultReconnectTries = 2
)

type Driver struct {
	Conn    []*Connection
	Timeout time.Duration
}

type Connection struct {
	Addr               string
	NumberOfConnection int
	PayloadCh          chan request

	workers []*SingleConnection

	closeOnce sync.Once
	closed    chan struct{}
}

type SingleConnection struct {
	addr           string
	communicatorCh chan request

	mu         sync.Mutex
	conn       net.Conn
	readBuffer []byte
}

type Result struct {
	Data []byte
	Err  error
}

type request struct {
	payload []byte
	result  chan Result
}

var resultPool = sync.Pool{
	New: func() any {
		return make(chan Result, 1)
	},
}

func New() (*Driver, error) {
	configuration := types.LoadConfiguration()

	if len(configuration.Server) == 0 {
		return nil, errors.New("no servers configured")
	}

	connections := make([]*Connection, 0, len(configuration.Server))

	for _, server := range configuration.Server {
		con, err := NewConnection(server.IpAddr, server.NumberOfConnection)
		if err != nil {
			return nil, err
		}

		connections = append(connections, con)
	}

	return &Driver{
		Conn:    connections,
		Timeout: defaultTimeout,
	}, nil
}

func NewConnection(addr string, numberConnection int) (*Connection, error) {
	if addr == "" {
		return nil, errors.New("server address is empty")
	}

	if numberConnection <= 0 {
		return nil, errors.New("number of connections must be greater than zero")
	}

	c := &Connection{
		Addr:               addr,
		NumberOfConnection: numberConnection,
		PayloadCh:          make(chan request, defaultQueueSize),
		workers:            make([]*SingleConnection, 0, numberConnection),
		closed:             make(chan struct{}),
	}

	if err := c.Init(); err != nil {
		_ = c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Connection) Init() error {
	for i := 0; i < c.NumberOfConnection; i++ {
		worker, err := NewSingleConnection(c.PayloadCh, c.Addr)
		if err != nil {
			_ = c.Close()
			return err
		}

		c.workers = append(c.workers, worker)
		go worker.Worker()
	}

	return nil
}

func NewSingleConnection(communicatorCh chan request, addr string) (*SingleConnection, error) {
	conn, err := net.DialTimeout("tcp", addr, defaultTimeout)
	if err != nil {
		return nil, err
	}

	configureTCP(conn)

	return &SingleConnection{
		addr:           addr,
		communicatorCh: communicatorCh,
		conn:           conn,
		readBuffer:     make([]byte, defaultReadBufferSize),
	}, nil
}

func configureTCP(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)

	_ = tcpConn.SetReadBuffer(defaultReadBufferSize)
	_ = tcpConn.SetWriteBuffer(defaultReadBufferSize)
}

func (s *SingleConnection) Worker() {
	for req := range s.communicatorCh {
		data, err := s.send(req.payload)

		req.result <- Result{
			Data: data,
			Err:  err,
		}
	}
}

func (s *SingleConnection) send(payload []byte) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= defaultReconnectTries; attempt++ {
		data, err := s.sendOnce(payload)
		if err == nil {
			return data, nil
		}

		lastErr = err

		if reconnectErr := s.reconnect(); reconnectErr != nil {
			lastErr = reconnectErr
		}

		time.Sleep(defaultReconnectDelay)
	}

	return nil, lastErr
}

func (s *SingleConnection) sendOnce(payload []byte) ([]byte, error) {
	if s.conn == nil {
		return nil, errors.New("connection is not available")
	}

	_, err := s.conn.Write(payload)
	if err != nil {
		return nil, err
	}

	n, err := s.conn.Read(s.readBuffer)
	if err != nil {
		if err == io.EOF {
			return nil, err
		}

		return nil, err
	}

	response := make([]byte, n)
	copy(response, s.readBuffer[:n])

	return response, nil
}

func (s *SingleConnection) reconnect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}

	conn, err := net.DialTimeout("tcp", s.addr, defaultTimeout)
	if err != nil {
		log.Println("client reconnect error:", err)
		return err
	}

	configureTCP(conn)

	s.conn = conn

	return nil
}

func (d *Driver) Set(key, value string, ttl int) ([]byte, error) {
	return d.SetBytes([]byte(key), []byte(value), ttl)
}

func (d *Driver) Get(key string) ([]byte, error) {
	return d.GetBytes([]byte(key))
}

func (d *Driver) Delete(key string) ([]byte, error) {
	return d.DeleteBytes([]byte(key))
}

func (d *Driver) SetBytes(key, value []byte, ttl int) ([]byte, error) {
	payload, err := p.Set(key, value, ttl)
	if err != nil {
		return nil, err
	}

	return d.operationSync(payload, d.route(key))
}

func (d *Driver) GetBytes(key []byte) ([]byte, error) {
	payload, err := p.Get(key)
	if err != nil {
		return nil, err
	}

	return d.operationSync(payload, d.route(key))
}

func (d *Driver) DeleteBytes(key []byte) ([]byte, error) {
	payload, err := p.Delete(key)
	if err != nil {
		return nil, err
	}

	return d.operationSync(payload, d.route(key))
}

func (d *Driver) SetAsync(key, value []byte, ttl int) (chan Result, error) {
	payload, err := p.Set(key, value, ttl)
	if err != nil {
		return nil, err
	}

	return d.operationAsync(payload, d.route(key))
}

func (d *Driver) GetAsync(key []byte) (chan Result, error) {
	payload, err := p.Get(key)
	if err != nil {
		return nil, err
	}

	return d.operationAsync(payload, d.route(key))
}

func (d *Driver) DeleteAsync(key []byte) (chan Result, error) {
	payload, err := p.Delete(key)
	if err != nil {
		return nil, err
	}

	return d.operationAsync(payload, d.route(key))
}

func (d *Driver) SetReq(key, value []byte, ttl int) (<-chan []byte, error) {
	resCh, err := d.SetAsync(key, value, ttl)
	if err != nil {
		return nil, err
	}

	return onlyData(resCh), nil
}

func (d *Driver) GetReq(key []byte) (<-chan []byte, error) {
	resCh, err := d.GetAsync(key)
	if err != nil {
		return nil, err
	}

	return onlyData(resCh), nil
}

func (d *Driver) DeleteReq(key []byte) (<-chan []byte, error) {
	resCh, err := d.DeleteAsync(key)
	if err != nil {
		return nil, err
	}

	return onlyData(resCh), nil
}

func (d *Driver) operationSync(payload []byte, route int) ([]byte, error) {
	if len(d.Conn) == 0 {
		return nil, errors.New("driver has no connections")
	}

	if route < 0 || route >= len(d.Conn) {
		return nil, errors.New("invalid route")
	}

	resultCh := resultPool.Get().(chan Result)

	req := request{
		payload: payload,
		result:  resultCh,
	}

	select {
	case d.Conn[route].PayloadCh <- req:
	default:
		resultPool.Put(resultCh)
		return nil, errors.New("request queue full")
	}

	res := <-resultCh

	resultPool.Put(resultCh)

	return res.Data, res.Err
}

func (d *Driver) operationAsync(payload []byte, route int) (chan Result, error) {
	if len(d.Conn) == 0 {
		return nil, errors.New("driver has no connections")
	}

	if route < 0 || route >= len(d.Conn) {
		return nil, errors.New("invalid route")
	}

	resultCh := make(chan Result, 1)

	req := request{
		payload: payload,
		result:  resultCh,
	}

	select {
	case d.Conn[route].PayloadCh <- req:
		return resultCh, nil

	default:
		return nil, errors.New("request queue full")
	}
}

func (d *Driver) route(key []byte) int {
	if len(d.Conn) == 1 {
		return 0
	}

	return int(fnv32a(key)) % len(d.Conn)
}

func fnv32a(data []byte) uint32 {
	var h uint32 = 2166136261

	for _, b := range data {
		h ^= uint32(b)
		h *= 16777619
	}

	return h
}

func onlyData(resCh <-chan Result) <-chan []byte {
	dataCh := make(chan []byte, 1)

	go func() {
		defer close(dataCh)

		res := <-resCh
		if res.Err != nil {
			log.Println("driver request error:", res.Err)
			return
		}

		dataCh <- res.Data
	}()

	return dataCh
}

func (d *Driver) Close() error {
	var finalErr error

	for _, conn := range d.Conn {
		if err := conn.Close(); err != nil {
			finalErr = err
		}
	}

	return finalErr
}

func (c *Connection) Close() error {
	var finalErr error

	c.closeOnce.Do(func() {
		close(c.closed)
		close(c.PayloadCh)

		for _, worker := range c.workers {
			if err := worker.Close(); err != nil {
				finalErr = err
			}
		}
	})

	return finalErr
}

func (s *SingleConnection) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		return nil
	}

	err := s.conn.Close()
	s.conn = nil

	return err
}
