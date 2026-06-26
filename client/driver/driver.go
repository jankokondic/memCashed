package client

import (
	"bufio"
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
	defaultQueueSize        = 65536
	defaultTimeout          = 5 * time.Second
	defaultReadBufferSize   = 1024*1024 + 64
	defaultWriteBufferSize  = 1024 * 1024
	defaultReconnectDelay   = 100 * time.Millisecond
	defaultReconnectTries   = 2
	defaultResponseHeaderSz = 4
)

type Mode int

const (
	ModeSync Mode = iota
	ModePipeline
)

type Driver struct {
	Conn    []*Connection
	Timeout time.Duration
	Mode    Mode
}

type Connection struct {
	Addr               string
	NumberOfConnection int
	PayloadCh          chan request
	Mode               Mode

	workers []*SingleConnection

	closeOnce sync.Once
	closed    chan struct{}
}

type SingleConnection struct {
	addr           string
	communicatorCh chan request
	mode           Mode

	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	closed chan struct{}
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
	return NewWithMode(ModeSync)
}

func NewWithMode(mode Mode) (*Driver, error) {
	configuration := types.LoadConfiguration()

	if len(configuration.Server) == 0 {
		return nil, errors.New("no servers configured")
	}

	connections := make([]*Connection, 0, len(configuration.Server))

	for _, server := range configuration.Server {
		con, err := NewConnectionWithMode(server.IpAddr, server.NumberOfConnection, mode)
		if err != nil {
			return nil, err
		}

		connections = append(connections, con)
	}

	return &Driver{
		Conn:    connections,
		Timeout: defaultTimeout,
		Mode:    mode,
	}, nil
}

func NewConnection(addr string, numberConnection int) (*Connection, error) {
	return NewConnectionWithMode(addr, numberConnection, ModeSync)
}

func NewConnectionWithMode(addr string, numberConnection int, mode Mode) (*Connection, error) {
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
		Mode:               mode,
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
		worker, err := NewSingleConnection(c.PayloadCh, c.Addr, c.Mode)
		if err != nil {
			_ = c.Close()
			return err
		}

		c.workers = append(c.workers, worker)

		if c.Mode == ModePipeline {
			go worker.PipelineWorker()
		} else {
			go worker.SyncWorker()
		}
	}

	return nil
}

func NewSingleConnection(communicatorCh chan request, addr string, mode Mode) (*SingleConnection, error) {
	conn, err := net.DialTimeout("tcp", addr, defaultTimeout)
	if err != nil {
		return nil, err
	}

	configureTCP(conn)

	return &SingleConnection{
		addr:           addr,
		communicatorCh: communicatorCh,
		mode:           mode,
		conn:           conn,
		reader:         bufio.NewReaderSize(conn, defaultReadBufferSize),
		writer:         bufio.NewWriterSize(conn, defaultWriteBufferSize),
		closed:         make(chan struct{}),
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
	_ = tcpConn.SetWriteBuffer(defaultWriteBufferSize)
}

func (s *SingleConnection) SyncWorker() {
	for req := range s.communicatorCh {
		data, err := s.sendSync(req.payload)

		req.result <- Result{
			Data: data,
			Err:  err,
		}
	}
}

func (s *SingleConnection) PipelineWorker() {
	pending := make(chan request, defaultQueueSize)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		s.pipelineWriter(pending)
	}()

	go func() {
		defer wg.Done()
		s.pipelineReader(pending)
	}()

	wg.Wait()
}

func (s *SingleConnection) pipelineWriter(pending chan<- request) {
	flushTicker := time.NewTicker(200 * time.Microsecond)
	defer flushTicker.Stop()
	defer close(pending)

	for {
		select {
		case req, ok := <-s.communicatorCh:
			if !ok {
				_ = s.writer.Flush()
				return
			}

			_, err := s.writer.Write(req.payload)
			if err != nil {
				req.result <- Result{Err: err}
				continue
			}

			pending <- req

			if s.writer.Buffered() >= 64*1024 {
				_ = s.writer.Flush()
			}

		case <-flushTicker.C:
			_ = s.writer.Flush()
		}
	}
}

func (s *SingleConnection) pipelineReader(pending <-chan request) {
	for req := range pending {
		data, err := readFramedResponse(s.reader)

		req.result <- Result{
			Data: data,
			Err:  err,
		}
	}
}

func (s *SingleConnection) sendSync(payload []byte) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= defaultReconnectTries; attempt++ {
		data, err := s.sendOnceFramed(payload)
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

func (s *SingleConnection) sendOnceFramed(payload []byte) ([]byte, error) {
	if s.conn == nil {
		return nil, errors.New("connection is not available")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.writer.Write(payload)
	if err != nil {
		return nil, err
	}

	err = s.writer.Flush()
	if err != nil {
		return nil, err
	}

	return readFramedResponse(s.reader)
}

func readFramedResponse(reader *bufio.Reader) ([]byte, error) {
	header := make([]byte, defaultResponseHeaderSz)

	_, err := io.ReadFull(reader, header)
	if err != nil {
		return nil, err
	}

	size := int(uint32(header[0]) |
		uint32(header[1])<<8 |
		uint32(header[2])<<16 |
		uint32(header[3])<<24)

	if size < 0 || size > defaultReadBufferSize {
		return nil, errors.New("invalid response size")
	}

	response := make([]byte, size)

	_, err = io.ReadFull(reader, response)
	if err != nil {
		return nil, err
	}

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
	s.reader = bufio.NewReaderSize(conn, defaultReadBufferSize)
	s.writer = bufio.NewWriterSize(conn, defaultWriteBufferSize)

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
	resCh, err := d.operationAsync(payload, route)
	if err != nil {
		return nil, err
	}

	res := <-resCh
	return res.Data, res.Err
}

func (d *Driver) operationAsync(payload []byte, route int) (chan Result, error) {
	if len(d.Conn) == 0 {
		return nil, errors.New("driver has no connections")
	}

	if route < 0 || route >= len(d.Conn) {
		return nil, errors.New("invalid route")
	}

	var resultCh chan Result

	if d.Mode == ModeSync {
		resultCh = resultPool.Get().(chan Result)
	} else {
		resultCh = make(chan Result, 1)
	}

	req := request{
		payload: payload,
		result:  resultCh,
	}

	select {
	case d.Conn[route].PayloadCh <- req:
		return resultCh, nil

	default:
		if d.Mode == ModeSync {
			resultPool.Put(resultCh)
		}

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
