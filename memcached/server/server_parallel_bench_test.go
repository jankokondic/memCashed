package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/memory_allocator"
)

var parallelBenchBlockSizes = []int{
	64,
	128,
	256,
	512,
	1024,
	2048,
	4096,
	8192,
	16384,
	32768,
	65536,
	131072,
	262144,
	524288,
	1048576,
}

func newParallelBenchmarkServer(workerCount int) *Server {
	allocator := memory_allocator.New(512 * constants.MiB)

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
		MaxConn:    100000,
		ActiveConn: 0,
		Manager:    memory_allocator.NewSlabManager(slabs, workerCount),
	}
}

func parallelKeyCountForBlockSize(blockSize int) int {
	switch {
	case blockSize >= 1048576:
		return 8
	case blockSize >= 524288:
		return 16
	case blockSize >= 262144:
		return 32
	case blockSize >= 131072:
		return 64
	case blockSize >= 65536:
		return 128
	case blockSize >= 16384:
		return 512
	default:
		return 2048
	}
}

func parallelBenchBody(size int) []byte {
	body := make([]byte, size)

	for i := range body {
		body[i] = byte('a' + i%26)
	}

	return body
}

func parallelBenchPayload(operation byte, key string, ttl uint32, body []byte) []byte {
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

func parallelBenchFrame(payload []byte) []byte {
	frame := make([]byte, constants.BufferSizeTCP+len(payload))

	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[constants.BufferSizeTCP:], payload)

	return frame
}

func parallelBenchWriteFrame(b *testing.B, conn net.Conn, frame []byte) {
	b.Helper()

	if _, err := conn.Write(frame); err != nil {
		b.Fatalf("write frame failed: %v", err)
	}
}

func parallelBenchReadResponse(b *testing.B, conn net.Conn, response []byte) {
	b.Helper()

	if _, err := io.ReadFull(conn, response); err != nil {
		b.Fatalf("read response failed: %v", err)
	}
}

func makeParallelKeys(blockSize int, prefix string) []string {
	keyCount := parallelKeyCountForBlockSize(blockSize)
	keys := make([]string, keyCount)

	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
	}

	return keys
}

func makeParallelSetFrames(b *testing.B, blockSize int, keys []string) [][]byte {
	b.Helper()

	frames := make([][]byte, len(keys))

	for i, key := range keys {
		bodySize := blockSize - constants.HeaderSize - len(key)
		if bodySize < 0 {
			bodySize = 0
		}

		body := parallelBenchBody(bodySize)
		payload := parallelBenchPayload(constants.SetOperation, key, 0, body)

		if len(payload) > blockSize {
			b.Fatalf("payload larger than block size: payload=%d block=%d", len(payload), blockSize)
		}

		frames[i] = parallelBenchFrame(payload)
	}

	return frames
}

func makeParallelGetFrames(keys []string) [][]byte {
	frames := make([][]byte, len(keys))

	for i, key := range keys {
		payload := parallelBenchPayload(constants.GetOperation, key, 0, nil)
		frames[i] = parallelBenchFrame(payload)
	}

	return frames
}

func makeParallelDeleteFrames(keys []string) [][]byte {
	frames := make([][]byte, len(keys))

	for i, key := range keys {
		payload := parallelBenchPayload(constants.DeleteOperation, key, 0, nil)
		frames[i] = parallelBenchFrame(payload)
	}

	return frames
}


func closeParallelClient(clientConn net.Conn, done chan struct{}) {
	_ = clientConn.Close()
	<-done
}

func preloadParallelServer(b *testing.B, s *Server, setFrames [][]byte) {
	b.Helper()

	clientConn, done := startServerPipeForParallelBench(s)
	defer closeParallelClient(clientConn, done)

	insertedResponse := make([]byte, len(constants.ObjectInserted))

	for _, frame := range setFrames {
		parallelBenchWriteFrame(b, clientConn, frame)
		parallelBenchReadResponse(b, clientConn, insertedResponse)
	}
}

func startServerPipeForParallelBench(s *Server) (net.Conn, chan struct{}) {
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})

	s.Lock()
	s.ActiveConn++
	s.Unlock()

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	return clientConn, done
}

func benchmarkParallelSet(b *testing.B, blockSize int, workers int) {
	s := newParallelBenchmarkServer(workers)

	keys := makeParallelKeys(blockSize, "p-set-k")
	setFrames := makeParallelSetFrames(b, blockSize, keys)
	insertedResponseSize := len(constants.ObjectInserted)

	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		clientConn, done := startServerPipeForParallelBench(s)
		defer closeParallelClient(clientConn, done)

		insertedResponse := make([]byte, insertedResponseSize)

		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			frame := setFrames[int(i)%len(setFrames)]

			parallelBenchWriteFrame(b, clientConn, frame)
			parallelBenchReadResponse(b, clientConn, insertedResponse)
		}
	})
}

func benchmarkParallelGet(b *testing.B, blockSize int, workers int) {
	s := newParallelBenchmarkServer(workers)

	keys := makeParallelKeys(blockSize, "p-get-k")
	setFrames := makeParallelSetFrames(b, blockSize, keys)
	getFrames := makeParallelGetFrames(keys)

	preloadParallelServer(b, s, setFrames)

	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		clientConn, done := startServerPipeForParallelBench(s)
		defer closeParallelClient(clientConn, done)

		getResponse := make([]byte, blockSize)

		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			frame := getFrames[int(i)%len(getFrames)]

			parallelBenchWriteFrame(b, clientConn, frame)
			parallelBenchReadResponse(b, clientConn, getResponse)
		}
	})
}

func benchmarkParallelSetGet(b *testing.B, blockSize int, workers int) {
	s := newParallelBenchmarkServer(workers)

	keys := makeParallelKeys(blockSize, "p-sg-k")
	setFrames := makeParallelSetFrames(b, blockSize, keys)
	getFrames := makeParallelGetFrames(keys)

	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		clientConn, done := startServerPipeForParallelBench(s)
		defer closeParallelClient(clientConn, done)

		insertedResponse := make([]byte, len(constants.ObjectInserted))
		getResponse := make([]byte, blockSize)

		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			pos := int(i) % len(setFrames)

			parallelBenchWriteFrame(b, clientConn, setFrames[pos])
			parallelBenchReadResponse(b, clientConn, insertedResponse)

			parallelBenchWriteFrame(b, clientConn, getFrames[pos])
			parallelBenchReadResponse(b, clientConn, getResponse)
		}
	})
}

func benchmarkParallelMixed(b *testing.B, blockSize int, workers int) {
	s := newParallelBenchmarkServer(workers)

	keys := makeParallelKeys(blockSize, "p-mixed-k")
	setFrames := makeParallelSetFrames(b, blockSize, keys)
	getFrames := makeParallelGetFrames(keys)
	deleteFrames := makeParallelDeleteFrames(keys)

	preloadParallelServer(b, s, setFrames)

	var counter uint64

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		clientConn, done := startServerPipeForParallelBench(s)
		defer closeParallelClient(clientConn, done)

		insertedResponse := make([]byte, len(constants.ObjectInserted))
		deletedResponse := make([]byte, len(constants.ObjectDeleted))
		getResponse := make([]byte, blockSize)

		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			pos := int(i) % len(keys)

			switch i % 10 {
			case 0, 1:
				parallelBenchWriteFrame(b, clientConn, setFrames[pos])
				parallelBenchReadResponse(b, clientConn, insertedResponse)

			case 2:
				parallelBenchWriteFrame(b, clientConn, deleteFrames[pos])
				parallelBenchReadResponse(b, clientConn, deletedResponse)

				parallelBenchWriteFrame(b, clientConn, setFrames[pos])
				parallelBenchReadResponse(b, clientConn, insertedResponse)

			default:
				parallelBenchWriteFrame(b, clientConn, getFrames[pos])
				parallelBenchReadResponse(b, clientConn, getResponse)
			}
		}
	})
}

func BenchmarkParallelServer_SET_WorkersCPU(b *testing.B) {
	workers := runtime.NumCPU()

	for _, size := range parallelBenchBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkParallelSet(b, size, workers)
		})
	}
}

func BenchmarkParallelServer_GET_WorkersCPU(b *testing.B) {
	workers := runtime.NumCPU()

	for _, size := range parallelBenchBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkParallelGet(b, size, workers)
		})
	}
}

func BenchmarkParallelServer_SET_GET_WorkersCPU(b *testing.B) {
	workers := runtime.NumCPU()

	for _, size := range parallelBenchBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkParallelSetGet(b, size, workers)
		})
	}
}

func BenchmarkParallelServer_MIXED_WorkersCPU(b *testing.B) {
	workers := runtime.NumCPU()

	for _, size := range parallelBenchBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkParallelMixed(b, size, workers)
		})
	}
}
