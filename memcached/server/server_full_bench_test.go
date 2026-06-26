package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/memory_allocator"
)

var benchmarkBlockSizes = []int{
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

func newFullBenchmarkServer(workerCount int) *Server {
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
		MaxConn:    100000,
		ActiveConn: 1,
		Manager:    memory_allocator.NewSlabManager(slabs, workerCount),
	}
}

func keyCountForBlockSize(blockSize int) int {
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
		return 1024
	}
}

func fullBenchBody(size int) []byte {
	body := make([]byte, size)

	for i := range body {
		body[i] = byte('a' + i%26)
	}

	return body
}

func fullBenchPayload(operation byte, key string, ttl uint32, body []byte) []byte {
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

func fullBenchFrame(payload []byte) []byte {
	frame := make([]byte, constants.BufferSizeTCP+len(payload))

	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[constants.BufferSizeTCP:], payload)

	return frame
}

func fullBenchWriteFrame(b *testing.B, conn net.Conn, frame []byte) {
	b.Helper()

	if _, err := conn.Write(frame); err != nil {
		b.Fatalf("write frame failed: %v", err)
	}
}

func fullBenchReadResponse(b *testing.B, conn net.Conn, response []byte) {
	b.Helper()

	if _, err := io.ReadFull(conn, response); err != nil {
		b.Fatalf("read response failed: %v", err)
	}
}

func closeFullBenchConn(clientConn net.Conn, done chan struct{}) {
	_ = clientConn.Close()
	<-done
}

func makeSetFramesForBlockSize(b *testing.B, blockSize int, keyPrefix string) [][]byte {
	b.Helper()

	keyCount := keyCountForBlockSize(blockSize)
	frames := make([][]byte, keyCount)

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)

		bodySize := blockSize - constants.HeaderSize - len(key)
		if bodySize < 0 {
			bodySize = 0
		}

		body := fullBenchBody(bodySize)
		payload := fullBenchPayload(constants.SetOperation, key, 0, body)

		if len(payload) > blockSize {
			b.Fatalf("payload larger than block size: payload=%d block=%d", len(payload), blockSize)
		}

		frames[i] = fullBenchFrame(payload)
	}

	return frames
}

func makeGetFramesForKeys(keys []string) [][]byte {
	frames := make([][]byte, len(keys))

	for i, key := range keys {
		payload := fullBenchPayload(constants.GetOperation, key, 0, nil)
		frames[i] = fullBenchFrame(payload)
	}

	return frames
}

func makeDeleteFramesForKeys(keys []string) [][]byte {
	frames := make([][]byte, len(keys))

	for i, key := range keys {
		payload := fullBenchPayload(constants.DeleteOperation, key, 0, nil)
		frames[i] = fullBenchFrame(payload)
	}

	return frames
}

func makeKeys(blockSize int, prefix string) []string {
	keyCount := keyCountForBlockSize(blockSize)
	keys := make([]string, keyCount)

	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
	}

	return keys
}

func preloadServer(b *testing.B, conn net.Conn, setFrames [][]byte, insertedResponse []byte) {
	b.Helper()

	for _, frame := range setFrames {
		fullBenchWriteFrame(b, conn, frame)
		fullBenchReadResponse(b, conn, insertedResponse)
	}
}

func benchmarkRealServerSet(b *testing.B, blockSize int, workers int) {
	s := newFullBenchmarkServer(workers)

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	setFrames := makeSetFramesForBlockSize(b, blockSize, "set-k")
	insertedResponse := make([]byte, len(constants.ObjectInserted))

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		frame := setFrames[i%len(setFrames)]

		fullBenchWriteFrame(b, clientConn, frame)
		fullBenchReadResponse(b, clientConn, insertedResponse)
	}

	b.StopTimer()
	closeFullBenchConn(clientConn, done)
}

func benchmarkRealServerGet(b *testing.B, blockSize int, workers int) {
	s := newFullBenchmarkServer(workers)

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	keys := makeKeys(blockSize, "get-k")
	setFrames := makeSetFramesForBlockSize(b, blockSize, "get-k")
	getFrames := makeGetFramesForKeys(keys)

	insertedResponse := make([]byte, len(constants.ObjectInserted))
	getResponse := make([]byte, blockSize)

	preloadServer(b, clientConn, setFrames, insertedResponse)

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		frame := getFrames[i%len(getFrames)]

		fullBenchWriteFrame(b, clientConn, frame)
		fullBenchReadResponse(b, clientConn, getResponse)
	}

	b.StopTimer()
	closeFullBenchConn(clientConn, done)
}

func benchmarkRealServerSetGet(b *testing.B, blockSize int, workers int) {
	s := newFullBenchmarkServer(workers)

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	keys := makeKeys(blockSize, "sg-k")
	setFrames := makeSetFramesForBlockSize(b, blockSize, "sg-k")
	getFrames := makeGetFramesForKeys(keys)

	insertedResponse := make([]byte, len(constants.ObjectInserted))
	getResponse := make([]byte, blockSize)

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pos := i % len(setFrames)

		fullBenchWriteFrame(b, clientConn, setFrames[pos])
		fullBenchReadResponse(b, clientConn, insertedResponse)

		fullBenchWriteFrame(b, clientConn, getFrames[pos])
		fullBenchReadResponse(b, clientConn, getResponse)
	}

	b.StopTimer()
	closeFullBenchConn(clientConn, done)
}

func benchmarkRealServerMixed(b *testing.B, blockSize int, workers int) {
	s := newFullBenchmarkServer(workers)

	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	keys := makeKeys(blockSize, "mixed-k")
	setFrames := makeSetFramesForBlockSize(b, blockSize, "mixed-k")
	getFrames := makeGetFramesForKeys(keys)
	deleteFrames := makeDeleteFramesForKeys(keys)

	insertedResponse := make([]byte, len(constants.ObjectInserted))
	deletedResponse := make([]byte, len(constants.ObjectDeleted))
	getResponse := make([]byte, blockSize)

	preloadServer(b, clientConn, setFrames, insertedResponse)

	b.ReportAllocs()
	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pos := i % len(keys)

		switch i % 10 {
		case 0, 1:
			fullBenchWriteFrame(b, clientConn, setFrames[pos])
			fullBenchReadResponse(b, clientConn, insertedResponse)

		case 2:
			fullBenchWriteFrame(b, clientConn, deleteFrames[pos])
			fullBenchReadResponse(b, clientConn, deletedResponse)

			fullBenchWriteFrame(b, clientConn, setFrames[pos])
			fullBenchReadResponse(b, clientConn, insertedResponse)

		default:
			fullBenchWriteFrame(b, clientConn, getFrames[pos])
			fullBenchReadResponse(b, clientConn, getResponse)
		}
	}

	b.StopTimer()
	closeFullBenchConn(clientConn, done)
}

func BenchmarkRealServer_SET_Workers1(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerSet(b, size, 1)
		})
	}
}

func BenchmarkRealServer_GET_Workers1(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerGet(b, size, 1)
		})
	}
}

func BenchmarkRealServer_SET_GET_Workers1(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerSetGet(b, size, 1)
		})
	}
}

func BenchmarkRealServer_MIXED_Workers1(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerMixed(b, size, 1)
		})
	}
}

func BenchmarkRealServer_SET_Workers4(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerSet(b, size, 4)
		})
	}
}

func BenchmarkRealServer_GET_Workers4(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerGet(b, size, 4)
		})
	}
}

func BenchmarkRealServer_SET_GET_Workers4(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerSetGet(b, size, 4)
		})
	}
}

func BenchmarkRealServer_MIXED_Workers4(b *testing.B) {
	for _, size := range benchmarkBlockSizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			benchmarkRealServerMixed(b, size, 4)
		})
	}
}