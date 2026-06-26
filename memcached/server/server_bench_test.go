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

func newBenchmarkServer(workerCount int) *Server {
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
	}

	return &Server{
		Add:        ":0",
		MaxConn:    100000,
		ActiveConn: 1,
		Manager:    memory_allocator.NewSlabManager(slabs, workerCount),
	}
}

func makeBenchmarkPayload(operation byte, key string, ttl uint32, body []byte) []byte {
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

func makeBenchmarkLengthPrefix(payloadSize int) []byte {
	buf := make([]byte, constants.BufferSizeTCP)
	binary.LittleEndian.PutUint32(buf[:4], uint32(payloadSize))
	return buf
}

func writeBenchmarkRequest(b *testing.B, conn net.Conn, payload []byte) {
	b.Helper()

	lengthPrefix := makeBenchmarkLengthPrefix(len(payload))

	if _, err := conn.Write(lengthPrefix); err != nil {
		b.Fatalf("failed to write length prefix: %v", err)
	}

	if _, err := conn.Write(payload); err != nil {
		b.Fatalf("failed to write payload: %v", err)
	}
}

func readBenchmarkResponse(b *testing.B, conn net.Conn, size int) {
	b.Helper()

	response := make([]byte, size)

	if _, err := io.ReadFull(conn, response); err != nil {
		b.Fatalf("failed to read response: %v", err)
	}
}

func benchmarkServerSet(b *testing.B, bodySize int, workerCount int) {
	s := newBenchmarkServer(workerCount)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	b.ReportAllocs()
	b.SetBytes(int64(bodySize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%d", i)
		payload := makeBenchmarkPayload(constants.SetOperation, key, 0, body)

		writeBenchmarkRequest(b, clientConn, payload)
		readBenchmarkResponse(b, clientConn, len(constants.ObjectInserted))
	}

	b.StopTimer()

	_ = clientConn.Close()
	<-done
}

func benchmarkServerSetGet(b *testing.B, bodySize int, workerCount int) {
	s := newBenchmarkServer(workerCount)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	keys := make([]string, b.N)

	b.StopTimer()

	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("k%d", i)

		setPayload := makeBenchmarkPayload(constants.SetOperation, keys[i], 0, body)

		writeBenchmarkRequest(b, clientConn, setPayload)
		readBenchmarkResponse(b, clientConn, len(constants.ObjectInserted))
	}

	b.StartTimer()
	b.ReportAllocs()
	b.SetBytes(int64(bodySize))

	for i := 0; i < b.N; i++ {
		getPayload := makeBenchmarkPayload(constants.GetOperation, keys[i], 0, nil)

		writeBenchmarkRequest(b, clientConn, getPayload)
		readBenchmarkResponse(b, clientConn, bodySize)
	}

	b.StopTimer()

	_ = clientConn.Close()
	<-done
}

func benchmarkServerMixedSetGetDelete(b *testing.B, bodySize int, workerCount int) {
	s := newBenchmarkServer(workerCount)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	body := make([]byte, bodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	preload := 10000
	keys := make([]string, preload)

	for i := 0; i < preload; i++ {
		keys[i] = fmt.Sprintf("pre%d", i)

		setPayload := makeBenchmarkPayload(constants.SetOperation, keys[i], 0, body)

		writeBenchmarkRequest(b, clientConn, setPayload)
		readBenchmarkResponse(b, clientConn, len(constants.ObjectInserted))
	}

	b.ReportAllocs()
	b.SetBytes(int64(bodySize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		switch i % 10 {
		case 0, 1:
			key := fmt.Sprintf("new%d", i)
			payload := makeBenchmarkPayload(constants.SetOperation, key, 0, body)

			writeBenchmarkRequest(b, clientConn, payload)
			readBenchmarkResponse(b, clientConn, len(constants.ObjectInserted))

		case 2:
			key := keys[i%preload]
			payload := makeBenchmarkPayload(constants.DeleteOperation, key, 0, nil)

			writeBenchmarkRequest(b, clientConn, payload)
			readBenchmarkResponse(b, clientConn, len(constants.ObjectDeleted))

			keys[i%preload] = fmt.Sprintf("re%d", i)

			setPayload := makeBenchmarkPayload(constants.SetOperation, keys[i%preload], 0, body)
			writeBenchmarkRequest(b, clientConn, setPayload)
			readBenchmarkResponse(b, clientConn, len(constants.ObjectInserted))

		default:
			key := keys[i%preload]
			payload := makeBenchmarkPayload(constants.GetOperation, key, 0, nil)

			writeBenchmarkRequest(b, clientConn, payload)
			readBenchmarkResponse(b, clientConn, bodySize)
		}
	}

	b.StopTimer()

	_ = clientConn.Close()
	<-done
}

func BenchmarkServerSet1KBWorkers1(b *testing.B) {
	benchmarkServerSet(b, 1024, 1)
}

func BenchmarkServerSet1KBWorkers4(b *testing.B) {
	benchmarkServerSet(b, 1024, 4)
}

func BenchmarkServerSetGet1KBWorkers1(b *testing.B) {
	benchmarkServerSetGet(b, 1024, 1)
}

func BenchmarkServerSetGet1KBWorkers4(b *testing.B) {
	benchmarkServerSetGet(b, 1024, 4)
}

func BenchmarkServerMixed1KBWorkers1(b *testing.B) {
	benchmarkServerMixedSetGetDelete(b, 1024, 1)
}

func BenchmarkServerMixed1KBWorkers4(b *testing.B) {
	benchmarkServerMixedSetGetDelete(b, 1024, 4)
}

func BenchmarkServerMixed64BWorkers4(b *testing.B) {
	benchmarkServerMixedSetGetDelete(b, 64, 4)
}

func BenchmarkServerMixed4KBWorkers4(b *testing.B) {
	benchmarkServerMixedSetGetDelete(b, 4096, 4)
}
