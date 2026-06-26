package server

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/memory_allocator"
)

func newTestServer() *Server {
	allocator := memory_allocator.New(16 * constants.MiB)

	slabs := []memory_allocator.Slab{
		memory_allocator.NewSlab(64, 0, allocator),
		memory_allocator.NewSlab(128, 0, allocator),
		memory_allocator.NewSlab(256, 0, allocator),
		memory_allocator.NewSlab(512, 0, allocator),
		memory_allocator.NewSlab(1024, 0, allocator),
		memory_allocator.NewSlab(2048, 0, allocator),
		memory_allocator.NewSlab(4096, 0, allocator),
	}

	return &Server{
		Add:        ":0",
		MaxConn:    10,
		ActiveConn: 1,
		Manager:    memory_allocator.NewSlabManager(slabs, 1),
	}
}

func makeServerTestPayload(operation byte, key string, ttl uint32, body []byte) []byte {
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

func makeLengthPrefix(payloadSize int) []byte {
	buf := make([]byte, constants.BufferSizeTCP)
	binary.LittleEndian.PutUint32(buf[:4], uint32(payloadSize))
	return buf
}

func writeRequest(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()

	lengthPrefix := makeLengthPrefix(len(payload))

	_, err := conn.Write(lengthPrefix)
	if err != nil {
		t.Fatalf("failed to write length prefix: %v", err)
	}

	_, err = conn.Write(payload)
	if err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}
}

func readResponse(t *testing.T, conn net.Conn, size int) []byte {
	t.Helper()

	response := make([]byte, size)

	_, err := io.ReadFull(conn, response)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	return response
}

func TestDecrease(t *testing.T) {
	s := &Server{
		ActiveConn: 3,
	}

	s.decrease()

	if s.ActiveConn != 2 {
		t.Fatalf("expected ActiveConn to be 2, got %d", s.ActiveConn)
	}
}

func TestClose(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	done := make(chan struct{})

	go func() {
		Close(serverConn, "test close")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return")
	}

	_, err := clientConn.Write([]byte("hello"))
	if err == nil {
		t.Fatal("expected error after connection was closed")
	}

	_ = clientConn.Close()
}

func TestHandleConnSetGetDeleteFlow(t *testing.T) {
	s := newTestServer()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	err := clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("failed to set client deadline: %v", err)
	}

	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	key := "name"
	value := []byte("janko")

	setPayload := makeServerTestPayload(constants.SetOperation, key, 0, value)
	writeRequest(t, clientConn, setPayload)

	setResponse := readResponse(t, clientConn, len(constants.ObjectInserted))
	if string(setResponse) != string(constants.ObjectInserted) {
		t.Fatalf("expected set response %q, got %q", string(constants.ObjectInserted), string(setResponse))
	}

	getPayload := makeServerTestPayload(constants.GetOperation, key, 0, nil)
	writeRequest(t, clientConn, getPayload)

	getResponse := readResponse(t, clientConn, len(value))
	if string(getResponse) != string(value) {
		t.Fatalf("expected get response %q, got %q", string(value), string(getResponse))
	}

	deletePayload := makeServerTestPayload(constants.DeleteOperation, key, 0, nil)
	writeRequest(t, clientConn, deletePayload)

	deleteResponse := readResponse(t, clientConn, len(constants.ObjectDeleted))
	if string(deleteResponse) != string(constants.ObjectDeleted) {
		t.Fatalf("expected delete response %q, got %q", string(constants.ObjectDeleted), string(deleteResponse))
	}

	getAgainPayload := makeServerTestPayload(constants.GetOperation, key, 0, nil)
	writeRequest(t, clientConn, getAgainPayload)

	getAgainResponse := readResponse(t, clientConn, len(constants.ErrObjectNotFound))
	if string(getAgainResponse) != string(constants.ErrObjectNotFound) {
		t.Fatalf("expected get again response %q, got %q", string(constants.ErrObjectNotFound), string(getAgainResponse))
	}

	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not stop after client close")
	}

	if s.ActiveConn != 0 {
		t.Fatalf("expected ActiveConn to be 0, got %d", s.ActiveConn)
	}
}

func TestHandleConnExpiredTTL(t *testing.T) {
	s := newTestServer()

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	err := clientConn.SetDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("failed to set client deadline: %v", err)
	}

	done := make(chan struct{})

	go func() {
		s.HandleConn(serverConn)
		close(done)
	}()

	key := "token"
	value := []byte("abc123")

	setPayload := makeServerTestPayload(constants.SetOperation, key, 1, value)
	writeRequest(t, clientConn, setPayload)

	setResponse := readResponse(t, clientConn, len(constants.ObjectInserted))
	if string(setResponse) != string(constants.ObjectInserted) {
		t.Fatalf("expected set response %q, got %q", string(constants.ObjectInserted), string(setResponse))
	}

	time.Sleep(1100 * time.Millisecond)

	getPayload := makeServerTestPayload(constants.GetOperation, key, 0, nil)
	writeRequest(t, clientConn, getPayload)

	getResponse := readResponse(t, clientConn, len(constants.ErrTimeExpire))
	if string(getResponse) != string(constants.ErrTimeExpire) {
		t.Fatalf("expected ttl response %q, got %q", string(constants.ErrTimeExpire), string(getResponse))
	}

	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleConn did not stop after client close")
	}
}
