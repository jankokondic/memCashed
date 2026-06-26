package server

import (
	"bufio"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/internal/types"
	"github.com/WatchJani/memCashed/memcached/memory_allocator"
	decoder "github.com/WatchJani/memCashed/memcached/parser"
)

const (
	readBufferSize  = 256 * 1024
	writeBufferSize = 256 * 1024
	maxPendingSize  = 4 * 1024 * 1024
)

type Server struct {
	Add        string
	MaxConn    int64
	ActiveConn int64
	Manager    *memory_allocator.SlabManager
}

func New() *Server {
	config := types.LoadConfiguration()
	newAllocator := config.MemoryAllocator()

	return &Server{
		Add:     config.Port(),
		MaxConn: int64(config.MaxConnection()),
		Manager: memory_allocator.NewSlabManager(
			config.Slabs(newAllocator),
			config.NumberWorker(),
		),
	}
}

func (s *Server) Run() error {
	ls, err := net.Listen(constants.TCP, s.Add)
	if err != nil {
		return err
	}
	defer Close(ls, constants.InfoServerClose)

	for {
		conn, err := ls.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		if atomic.AddInt64(&s.ActiveConn, 1) > s.MaxConn {
			atomic.AddInt64(&s.ActiveConn, -1)
			_ = conn.Close()
			continue
		}

		configureTCP(conn)

		go s.HandleConn(conn)
	}
}

func configureTCP(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	_ = tcpConn.SetReadBuffer(1024 * 1024)
	_ = tcpConn.SetWriteBuffer(1024 * 1024)
}

func (s *Server) decrease() {
	atomic.AddInt64(&s.ActiveConn, -1)
}

func Close(c io.Closer, msg string) {
	if err := c.Close(); err != nil {
		log.Println(err)
	}
}

func (s *Server) HandleConn(conn net.Conn) {
	defer func() {
		Close(conn, constants.InfoConnectionClose)
		s.decrease()
	}()

	writer := bufio.NewWriterSize(conn, writeBufferSize)

	readBuf := make([]byte, readBufferSize)
	pending := make([]byte, 0, readBufferSize)

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			return
		}

		pending = append(pending, readBuf[:n]...)

		processed := 0

		for {
			if len(pending)-processed < constants.BufferSizeTCP {
				break
			}

			headerStart := processed
			headerEnd := headerStart + constants.BufferSizeTCP

			payloadSize := decoder.DecodeLength(pending[headerStart:headerEnd])
			totalSize := constants.BufferSizeTCP + payloadSize

			if len(pending)-processed < totalSize {
				break
			}

			payloadStart := headerEnd
			payloadEnd := processed + totalSize

			slabBlock, index, err := s.Manager.GetSlab(payloadSize, nil)
			if err != nil {
				log.Println(err)
				processed += totalSize
				continue
			}

			payload := slabBlock[:payloadSize]
			copy(payload, pending[payloadStart:payloadEnd])

			s.Manager.Process(memory_allocator.NewTransfer(payload, index, writer))

			processed += totalSize
		}

		if processed > 0 {
			if err := writer.Flush(); err != nil {
				log.Println(err)
				return
			}

			if processed == len(pending) {
				pending = pending[:0]
			} else {
				copy(pending, pending[processed:])
				pending = pending[:len(pending)-processed]
			}
		}

		if cap(pending) > maxPendingSize {
			newPending := make([]byte, len(pending), readBufferSize)
			copy(newPending, pending)
			pending = newPending
		}
	}
}
