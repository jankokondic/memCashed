package memory_allocator

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/WatchJani/memCashed/memcached/constants"
)

type testWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buf.Write(p)
}

func (w *testWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	out := make([]byte, w.buf.Len())
	copy(out, w.buf.Bytes())

	return out
}

func (w *testWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buf.String()
}

func newTestSlabManager() *SlabManager {
	allocator := New(16 * constants.MiB)

	slabs := []Slab{
		NewSlab(64, 0, allocator),
		NewSlab(128, 0, allocator),
		NewSlab(256, 0, allocator),
		NewSlab(512, 0, allocator),
		NewSlab(1024, 0, allocator),
		NewSlab(2048, 0, allocator),
		NewSlab(4096, 0, allocator),
		NewSlab(8192, 0, allocator),
		NewSlab(16384, 0, allocator),
		NewSlab(32768, 0, allocator),
		NewSlab(65536, 0, allocator),
	}

	return NewSlabManager(slabs, 0)
}

func makeTestPayload(operation byte, key string, ttl uint32, body []byte) []byte {
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

func waitForBytes(t *testing.T, writer *testWriter, expected []byte) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if bytes.Equal(writer.Bytes(), expected) {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("expected writer bytes %q, got %q", string(expected), writer.String())
}

func TestWorkerSetGetDeleteFlow(t *testing.T) {
	manager := newTestSlabManager()

	go manager.Worker()

	key := "name"
	value := []byte("janko")

	setPayload := makeTestPayload(constants.SetOperation, key, 0, value)

	setBlock, setIndex, err := manager.GetSlab(len(setPayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for set failed: %v", err)
	}

	copy(setBlock, setPayload)

	setWriter := &testWriter{}

	manager.JobCh <- NewTransfer(setBlock, setIndex, setWriter)

	waitForBytes(t, setWriter, constants.ObjectInserted)

	getPayload := makeTestPayload(constants.GetOperation, key, 0, nil)

	getBlock, getIndex, err := manager.GetSlab(len(getPayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for get failed: %v", err)
	}

	copy(getBlock, getPayload)

	getWriter := &testWriter{}

	manager.JobCh <- NewTransfer(getBlock, getIndex, getWriter)

	waitForBytes(t, getWriter, value)

	deletePayload := makeTestPayload(constants.DeleteOperation, key, 0, nil)

	deleteBlock, deleteIndex, err := manager.GetSlab(len(deletePayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for delete failed: %v", err)
	}

	copy(deleteBlock, deletePayload)

	deleteWriter := &testWriter{}

	manager.JobCh <- NewTransfer(deleteBlock, deleteIndex, deleteWriter)

	waitForBytes(t, deleteWriter, constants.ObjectDeleted)

	getAgainPayload := makeTestPayload(constants.GetOperation, key, 0, nil)

	getAgainBlock, getAgainIndex, err := manager.GetSlab(len(getAgainPayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for get again failed: %v", err)
	}

	copy(getAgainBlock, getAgainPayload)

	getAgainWriter := &testWriter{}

	manager.JobCh <- NewTransfer(getAgainBlock, getAgainIndex, getAgainWriter)

	waitForBytes(t, getAgainWriter, constants.ErrObjectNotFound)
}

func TestChooseOperationSetGetDeleteFlow(t *testing.T) {
	manager := newTestSlabManager()

	key := "city"
	value := []byte("koper")

	setPayload := makeTestPayload(constants.SetOperation, key, 0, value)

	setBlock, setIndex, err := manager.GetSlab(len(setPayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for set failed: %v", err)
	}

	copy(setBlock, setPayload)

	setWriter := &testWriter{}
	manager.chooseOperation(NewTransfer(setBlock, setIndex, setWriter))

	if !bytes.Equal(setWriter.Bytes(), constants.ObjectInserted) {
		t.Fatalf("expected %q, got %q", string(constants.ObjectInserted), setWriter.String())
	}

	getPayload := makeTestPayload(constants.GetOperation, key, 0, nil)

	getBlock, getIndex, err := manager.GetSlab(len(getPayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for get failed: %v", err)
	}

	copy(getBlock, getPayload)

	getWriter := &testWriter{}
	manager.chooseOperation(NewTransfer(getBlock, getIndex, getWriter))

	if !bytes.Equal(getWriter.Bytes(), value) {
		t.Fatalf("expected %q, got %q", string(value), getWriter.String())
	}

	deletePayload := makeTestPayload(constants.DeleteOperation, key, 0, nil)

	deleteBlock, deleteIndex, err := manager.GetSlab(len(deletePayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for delete failed: %v", err)
	}

	copy(deleteBlock, deletePayload)

	deleteWriter := &testWriter{}
	manager.chooseOperation(NewTransfer(deleteBlock, deleteIndex, deleteWriter))

	if !bytes.Equal(deleteWriter.Bytes(), constants.ObjectDeleted) {
		t.Fatalf("expected %q, got %q", string(constants.ObjectDeleted), deleteWriter.String())
	}
}

func TestWorkerExpiredTTL(t *testing.T) {
	manager := newTestSlabManager()

	go manager.Worker()

	key := "token"
	value := []byte("abc123")

	setPayload := makeTestPayload(constants.SetOperation, key, 1, value)

	setBlock, setIndex, err := manager.GetSlab(len(setPayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for set failed: %v", err)
	}

	copy(setBlock, setPayload)

	setWriter := &testWriter{}
	manager.JobCh <- NewTransfer(setBlock, setIndex, setWriter)

	waitForBytes(t, setWriter, constants.ObjectInserted)

	time.Sleep(1100 * time.Millisecond)

	getPayload := makeTestPayload(constants.GetOperation, key, 0, nil)

	getBlock, getIndex, err := manager.GetSlab(len(getPayload), nil)
	if err != nil {
		t.Fatalf("GetSlab for get failed: %v", err)
	}

	copy(getBlock, getPayload)

	getWriter := &testWriter{}
	manager.JobCh <- NewTransfer(getBlock, getIndex, getWriter)

	waitForBytes(t, getWriter, constants.ErrTimeExpire)
}
