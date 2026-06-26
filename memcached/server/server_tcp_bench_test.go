package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/memory_allocator"
)

var tcpBenchBlockSizes = []int{
	64, 128, 256, 512,
	1024, 2048, 4096, 8192,
	16384, 32768, 65536,
	131072, 262144, 524288, 1048576,
}

func newTCPBenchmarkServer(workerCount int) *Server {
	allocator := memory_allocator.New(256 * constants.MiB)

	slabs := []memory_allocator.Slab{
		memory_allocator.NewSlab(64, 0, allocator),
		memory_allocator.NewSlab(128, 0, allocator),
		memory_allocator.NewSlab(256, 0, allocator),
		memory_allocator.NewSlab(512, 0, allocator),
		memory_allocator.NewSlab(1024, 0, allocator),
		memory_allocator.NewSlab(2048, 0, allocator),
		memory_allocator.NewSlab(4096, 0, allocator),
		memory_allocator.NewSlab(8192, 0, allocator),
		memory_allocator.NewSlab(16384, 0, allocator),
		memory_allocator.NewSlab(32768, 0, allocator),
		memory_allocator.NewSlab(65536, 0, allocator),
		memory_allocator.NewSlab(131072, 0, allocator),
		memory_allocator.NewSlab(262144, 0, allocator),
		memory_allocator.NewSlab(524288, 0, allocator),
		memory_allocator.NewSlab(1048576, 0, allocator),
	}

	return &Server{
		Add:        ":0",
		MaxConn:    1000000,
		ActiveConn: 0,
		Manager:    memory_allocator.NewSlabManager(slabs, workerCount),
	}
}

func tcpKeyCount(blockSize int) int {
	switch {
	case blockSize >= 1048576:
		return 4
	case blockSize >= 524288:
		return 8
	case blockSize >= 262144:
		return 16
	case blockSize >= 131072:
		return 32
	case blockSize >= 65536:
		return 64
	case blockSize >= 32768:
		return 128
	case blockSize >= 16384:
		return 256
	default:
		return 2048
	}
}

func tcpPayload(operation byte, key string, ttl uint32, body []byte) []byte {
	keyBytes := []byte(key)
	payload := make([]byte, constants.HeaderSize+len(keyBytes)+len(body))

	payload[0] = operation
	payload[1] = byte(len(keyBytes))
	binary.LittleEndian.PutUint32(payload[2:6], ttl)
	binary.LittleEndian.PutUint32(payload[6:10], uint32(len(body)))

	copy(payload[constants.HeaderSize:constants.HeaderSize+len(keyBytes)], keyBytes)
	copy(payload[constants.HeaderSize+len(keyBytes):], body)

	return payload
}

func tcpFrame(payload []byte) []byte {
	frame := make([]byte, constants.BufferSizeTCP+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[constants.BufferSizeTCP:], payload)
	return frame
}

func tcpBody(size int) []byte {
	body := make([]byte, size)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	return body
}

func tcpKeys(blockSize int, prefix string) []string {
	keys := make([]string, tcpKeyCount(blockSize))
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return keys
}

func tcpSetFrames(b *testing.B, blockSize int, keys []string) [][]byte {
	b.Helper()

	frames := make([][]byte, len(keys))

	for i, key := range keys {
		bodySize := blockSize - constants.HeaderSize - len(key)
		if bodySize < 0 {
			bodySize = 0
		}

		payload := tcpPayload(constants.SetOperation, key, 0, tcpBody(bodySize))

		if len(payload) > blockSize {
			b.Fatalf("payload larger than block: payload=%d block=%d", len(payload), blockSize)
		}

		frames[i] = tcpFrame(payload)
	}

	return frames
}

func tcpGetFrames(keys []string) [][]byte {
	frames := make([][]byte, len(keys))

	for i, key := range keys {
		frames[i] = tcpFrame(tcpPayload(constants.GetOperation, key, 0, nil))
	}

	return frames
}

func tcpDeleteFrames(keys []string) [][]byte {
	frames := make([][]byte, len(keys))

	for i, key := range keys {
		frames[i] = tcpFrame(tcpPayload(constants.DeleteOperation, key, 0, nil))
	}

	return frames
}

func startTCPBenchmarkServer(b *testing.B, workers int) (addr string, stop func()) {
	b.Helper()

	s := newTCPBenchmarkServer(workers)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen failed: %v", err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			s.Lock()
			s.ActiveConn++
			s.Unlock()

			go s.HandleConn(conn)
		}
	}()

	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func tcpDial(b *testing.B, addr string) net.Conn {
	b.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	return conn
}

func tcpWriteFrame(b *testing.B, conn net.Conn, frame []byte) {
	b.Helper()

	if _, err := conn.Write(frame); err != nil {
		b.Fatalf("write failed: %v", err)
	}
}

func tcpReadResponse(b *testing.B, conn net.Conn, response []byte) {
	b.Helper()

	if _, err := io.ReadFull(conn, response); err != nil {
		b.Fatalf("read failed: %v", err)
	}
}

func tcpPreload(b *testing.B, addr string, setFrames [][]byte) {
	b.Helper()

	conn := tcpDial(b, addr)
	defer conn.Close()

	response := make([]byte, len(constants.ObjectInserted))

	for _, frame := range setFrames {
		tcpWriteFrame(b, conn, frame)
		tcpReadResponse(b, conn, response)
	}
}

func benchmarkTCPSet(b *testing.B, blockSize int, workers int) {
	addr, stop := startTCPBenchmarkServer(b, workers)
	defer stop()

	keys := tcpKeys(blockSize, "tcp-set-k")
	setFrames := tcpSetFrames(b, blockSize, keys)

	b.SetParallelism(4)
	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn := tcpDial(b, addr)
		defer conn.Close()

		response := make([]byte, len(constants.ObjectInserted))
		i := 0

		for pb.Next() {
			frame := setFrames[i%len(setFrames)]
			i++

			tcpWriteFrame(b, conn, frame)
			tcpReadResponse(b, conn, response)
		}
	})
}

func benchmarkTCPGet(b *testing.B, blockSize int, workers int) {
	addr, stop := startTCPBenchmarkServer(b, workers)
	defer stop()

	keys := tcpKeys(blockSize, "tcp-get-k")
	setFrames := tcpSetFrames(b, blockSize, keys)
	getFrames := tcpGetFrames(keys)

	tcpPreload(b, addr, setFrames)

	b.SetParallelism(4)
	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn := tcpDial(b, addr)
		defer conn.Close()

		response := make([]byte, blockSize)
		i := 0

		for pb.Next() {
			frame := getFrames[i%len(getFrames)]
			i++

			tcpWriteFrame(b, conn, frame)
			tcpReadResponse(b, conn, response)
		}
	})
}

func benchmarkTCPMixed(b *testing.B, blockSize int, workers int) {
	addr, stop := startTCPBenchmarkServer(b, workers)
	defer stop()

	keys := tcpKeys(blockSize, "tcp-mixed-k")
	setFrames := tcpSetFrames(b, blockSize, keys)
	getFrames := tcpGetFrames(keys)
	deleteFrames := tcpDeleteFrames(keys)

	tcpPreload(b, addr, setFrames)

	b.SetParallelism(4)
	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn := tcpDial(b, addr)
		defer conn.Close()

		insertedResponse := make([]byte, len(constants.ObjectInserted))
		deletedResponse := make([]byte, len(constants.ObjectDeleted))
		getResponse := make([]byte, blockSize)

		i := 0

		for pb.Next() {
			pos := i % len(keys)

			switch i % 10 {
			case 0, 1:
				tcpWriteFrame(b, conn, setFrames[pos])
				tcpReadResponse(b, conn, insertedResponse)

			case 2:
				tcpWriteFrame(b, conn, deleteFrames[pos])
				tcpReadResponse(b, conn, deletedResponse)

				tcpWriteFrame(b, conn, setFrames[pos])
				tcpReadResponse(b, conn, insertedResponse)

			default:
				tcpWriteFrame(b, conn, getFrames[pos])
				tcpReadResponse(b, conn, getResponse)
			}

			i++
		}
	})
}

func BenchmarkTCPServer_SET_WorkersCPU(b *testing.B) {
	workers := runtime.NumCPU()

	for _, size := range tcpBenchBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkTCPSet(b, size, workers)
		})
	}
}

func BenchmarkTCPServer_GET_WorkersCPU(b *testing.B) {
	workers := runtime.NumCPU()

	for _, size := range tcpBenchBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkTCPGet(b, size, workers)
		})
	}
}

func BenchmarkTCPServer_MIXED_WorkersCPU(b *testing.B) {
	workers := runtime.NumCPU()

	for _, size := range tcpBenchBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkTCPMixed(b, size, workers)
		})
	}
}
