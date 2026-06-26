package server

import (
	"io"
	"log"
	"net"
	"sync"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/internal/types"
	"github.com/WatchJani/memCashed/memcached/memory_allocator"
	decoder "github.com/WatchJani/memCashed/memcached/parser"
)

// Server represents a server that handles TCP connections, manages active connections,
// and uses a memory allocator for efficient data handling.
type Server struct {
	Add        string // Address and port the server binds to.
	MaxConn    int    // Maximum number of allowed active connections.
	ActiveConn int    // Current number of active connections.
	sync.RWMutex
	Manager *memory_allocator.SlabManager // Memory allocator for managing slab memory.
}

// New initializes a new Server instance by loading the configuration
// and setting up the slab memory allocator.
func New() *Server {
	// Load server configuration.
	config := types.LoadConfiguration()

	// Initialize the memory allocator using the configuration.
	newAllocator := config.MemoryAllocator()

	// Create a new Server instance with the provided configuration and memory manager.
	return &Server{
		Add:     config.Port(),
		MaxConn: config.MaxConnection(),
		Manager: memory_allocator.NewSlabManager(
			config.Slabs(newAllocator), // Initialize the slab memory with the configured settings.
			config.NumberWorker(),      // Set the number of workers for slab management.
		),
	}
}

// Run starts the server, listens for incoming TCP connections,
// and handles them concurrently. It also enforces a maximum connection limit.
func (s *Server) Run() error {
	// Start listening for incoming TCP connections on the specified address.
	ls, err := net.Listen(constants.TCP, s.Add)
	if err != nil {
		return err // Return error if the server fails to start listening.
	}

	// Ensure the listener is closed properly when the function ends.
	defer Close(ls, constants.InfoServerClose)

	// Infinite loop to accept and handle incoming connections.
	for {
		conn, err := ls.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		s.Lock()

		if s.ActiveConn >= s.MaxConn {
			s.Unlock()

			if err := conn.Close(); err != nil {
				log.Println(err)
			}

			continue
		}

		s.ActiveConn++
		s.Unlock()

		go s.HandleConn(conn)
	}
}

// decrease reduces the active connection count by one.
// This method ensures thread-safety using a write lock.
func (s *Server) decrease() {
	s.Lock()         // Lock the server to ensure no race conditions.
	defer s.Unlock() // Ensure the lock is released after the function completes.

	// Decrease the active connection count.
	s.ActiveConn--
}

// Close safely closes an io.Closer resource (e.g., a connection or listener)
// and logs an optional message if closing fails.
func Close(c io.Closer, msg string) {
	if err := c.Close(); err != nil {
		log.Println(err)
	}
}

// HandleConn processes an individual TCP connection, reading data,
// allocating slab memory, and delegating requests to a job channel.
func (s *Server) HandleConn(conn net.Conn) {
	defer func() {
		Close(conn, constants.InfoConnectionClose)
		s.decrease()
	}()

	bufSize := make([]byte, constants.BufferSizeTCP)

	for {
		_, err := io.ReadFull(conn, bufSize)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}

		payloadSize := decoder.DecodeLength(bufSize)

		slabBlock, index, err := s.Manager.GetSlab(payloadSize, conn)
		if err != nil {
			log.Println(err)
			continue
		}

		_, err = io.ReadFull(conn, slabBlock[:payloadSize])
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			break
		}

		s.Req(slabBlock[:payloadSize], index, conn)
	}
}

// Req sends a processed request to the slab manager's job channel,
// including the payload, index, and connection.
func (s *Server) Req(buf []byte, index int, conn net.Conn) {
	// Create a new transfer object and send it to the job channel for further processing.
	s.Manager.Dispatch(memory_allocator.NewTransfer(buf, index, conn))
}
