package memory_allocator

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/link_list"
	"github.com/WatchJani/memCashed/memcached/stack"
)

// keyStoreShardCount must be a power of two (mask-based indexing).
const keyStoreShardCount = 256

// keyStore is a sharded map protecting against sync.Map's interface{} boxing
// on every Store/Load of a Key value.
type keyStore struct {
	shards [keyStoreShardCount]keyShard
}

type keyShard struct {
	mu sync.RWMutex
	m  map[string]Key
}

func newKeyStore() *keyStore {
	ks := &keyStore{}
	for i := range ks.shards {
		ks.shards[i].m = make(map[string]Key)
	}
	return ks
}

func (ks *keyStore) getShard(key string) *keyShard {
	h := link_list.Fnv32aString(key)
	return &ks.shards[h&(keyStoreShardCount-1)]
}

func (ks *keyStore) Load(key string) (Key, bool) {
	shard := ks.getShard(key)
	shard.mu.RLock()
	v, ok := shard.m[key]
	shard.mu.RUnlock()
	return v, ok
}

func (ks *keyStore) Store(key string, value Key) {
	shard := ks.getShard(key)
	shard.mu.Lock()
	shard.m[key] = value
	shard.mu.Unlock()
}

func (ks *keyStore) Delete(key string) {
	shard := ks.getShard(key)
	shard.mu.Lock()
	delete(shard.m, key)
	shard.mu.Unlock()
}

// Swap atomically replaces the value for key and returns whatever was
// there before (if anything), all under a single shard lock. Using this
// instead of separate Load+Store calls is what prevents two concurrent
// SETs on the same key from both observing the same "old" entry and
// double-freeing/double-releasing it.
func (ks *keyStore) Swap(key string, newValue Key) (Key, bool) {
	shard := ks.getShard(key)

	shard.mu.Lock()
	old, ok := shard.m[key]
	shard.m[key] = newValue
	shard.mu.Unlock()

	return old, ok
}

// CompareAndDelete removes key only if its current node pointer still
// matches old.pointer, i.e. nobody else already replaced or deleted the
// entry between our Load and this call. Returns whether it actually deleted.
func (ks *keyStore) CompareAndDelete(key string, old Key) bool {
	shard := ks.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	cur, ok := shard.m[key]
	if !ok || cur.pointer != old.pointer {
		return false
	}

	delete(shard.m, key)
	return true
}

// SlabManager manages slabs, LRU (Least Recently Used) caches, and memory allocation.
type SlabManager struct {
	slabs []Slab
	lru   []*link_list.ShardedLRU
	store *keyStore
}

// Transfer represents a data payload and connection information for a transfer task.
// Transfer represents a data payload and connection information for a transfer task.
type Transfer struct {
	payload []byte
	conn    io.Writer
	index   int
}

// Key represents a stored object with its field, TTL, and a pointer to its LRU node.
type Key struct {
	field     []byte
	ttl       time.Time
	pointer   *link_list.Node
	slabIndex int
}

// NewTransfer creates a new Transfer object with the specified payload, index, and connection.
func NewTransfer(payload []byte, index int, conn io.Writer) Transfer {
	return Transfer{
		payload: payload,
		conn:    conn,
		index:   index,
	}
}
func (s *SlabManager) GetSlabIndex(index int) *Slab {
	return &s.slabs[index]
}

func (s *SlabManager) GetLRUIndex(index int) *link_list.ShardedLRU {
	return s.lru[index]
}

// NewSlabManager creates a new SlabManager with the provided slabs.
//
// numberOfWorker is kept for API compatibility only. The old JobCh/Worker
// goroutine pool was dead code — Process() is always called synchronously
// from HandleConn, so a background worker pool never received any jobs and
// just sat there parked on an empty channel. It has been removed.
func NewSlabManager(slabs []Slab, numberOfWorker int) *SlabManager {
	_ = numberOfWorker

	lrus := make([]*link_list.ShardedLRU, len(slabs))
	for i := range lrus {
		lrus[i] = link_list.NewShardedLRU()
	}

	return &SlabManager{
		slabs: slabs,
		lru:   lrus,
		store: newKeyStore(),
	}
}

// GetSlab allocates a slab of memory based on the payload size, handles errors, and frees space if necessary.
func (s *SlabManager) GetSlab(payloadSize int, conn net.Conn) ([]byte, int, error) {
	slabIndex, chunkSize := s.GetIndex(payloadSize)

	slabBlock, err := s.ChoseSlab(slabIndex).AllocateMemory()
	if err == nil {
		return slabBlock, slabIndex, nil
	}

	if !s.GetSlabIndex(slabIndex).IsSlabActive() {
		if conn != nil {
			_, _ = conn.Write([]byte(err.Error()))

			_, readErr := conn.Read(constants.NoReq)
			if readErr != nil {
				return nil, -1, readErr
			}
		}

		return nil, slabIndex, err
	}

	slabBlock, key, ok := s.lru[slabIndex].PopLastFreeSpace(chunkSize)
	if !ok {
		return nil, slabIndex, fmt.Errorf("there is not enough space and LRU is empty")
	}

	s.store.Delete(key)

	return slabBlock, slabIndex, nil
}

func TLLParser(ttl uint32) time.Time {
	if ttl > 0 {
		return time.Now().Add(time.Duration(ttl) * time.Second)
	}
	return time.Time{}
}

func (s *SlabManager) GetIndex(dataSize int) (int, int) {
	low, high := 0, len(s.slabs)-1
	result := high
	slabs := s.slabs

	for low <= high {
		mid := low + (high-low)/2
		if slabs[mid].slabSize >= dataSize {
			result = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return result, slabs[result].slabSize
}

func (s *SlabManager) ChoseSlab(index int) *Slab {
	return &s.slabs[index]
}

// Slab represents a memory slab used for allocation.
//
// AllocateMemory's hot path is a lock-free bump allocator: pageMu.RLock()
// lets many goroutines bump pagePointer concurrently via atomic.AddInt64;
// pageMu.Lock() (exclusive) is only taken on the rare page-refill path.
type Slab struct {
	slabSize    int
	freeList    stack.Stack[unsafe.Pointer]
	freeMu      sync.Mutex // guards freeList only
	currentPage []byte
	pagePointer int64        // atomic byte offset into currentPage
	pageMu      sync.RWMutex // guards currentPage swaps
	*Allocator
}

func (s *Slab) IsSlabActive() bool {
	s.pageMu.RLock()
	active := s.currentPage != nil
	s.pageMu.RUnlock()
	return active
}

func (s *Slab) GetCurrentPage() []byte {
	s.pageMu.RLock()
	defer s.pageMu.RUnlock()
	return s.currentPage
}

func NewSlab(slabSize, maxMemoryAllocate int, allocator *Allocator) Slab {
	return Slab{
		slabSize:  slabSize,
		freeList:  stack.New[unsafe.Pointer](10),
		Allocator: allocator,
	}
}

// AllocateMemory allocates memory for the slab: freelist reuse first,
// then a lock-free bump into the current page, and only takes the
// exclusive page lock when a new page actually needs to be mapped in.
func (s *Slab) AllocateMemory() ([]byte, error) {
	// 1. Freelist reuse — separate, tiny lock, doesn't block the bump path.
	s.freeMu.Lock()
	if !s.freeList.IsEmpty() {
		ptr, err := s.freeList.Pop()
		s.freeMu.Unlock()
		if err != nil {
			return nil, err
		}
		return unsafe.Slice((*byte)(ptr), s.slabSize), nil
	}
	s.freeMu.Unlock()

	// 2. Lock-free bump path.
	s.pageMu.RLock()
	if s.currentPage != nil {
		page := s.currentPage
		size := int64(s.slabSize)
		start := atomic.AddInt64(&s.pagePointer, size) - size
		end := start + size
		if end <= int64(len(page)) {
			block := page[start:end]
			s.pageMu.RUnlock()
			return block, nil
		}
		// Over-shot: roll back our reservation, fall through to refill.
		atomic.AddInt64(&s.pagePointer, -size)
	}
	s.pageMu.RUnlock()

	// 3. Slow path: refill the page under the exclusive lock.
	s.pageMu.Lock()
	defer s.pageMu.Unlock()

	// Someone else may have refilled it already — try once more.
	if s.currentPage != nil {
		size := int64(s.slabSize)
		start := atomic.AddInt64(&s.pagePointer, size) - size
		end := start + size
		if end <= int64(len(s.currentPage)) {
			return s.currentPage[start:end], nil
		}
		atomic.AddInt64(&s.pagePointer, -size)
	}

	block, err := s.AllocateBlock()
	if err != nil {
		return nil, err
	}

	s.currentPage = block
	atomic.StoreInt64(&s.pagePointer, int64(s.slabSize))

	return block[:s.slabSize], nil
}

func (s *Slab) UpdatePage(dataBlock []byte) {
	s.pageMu.Lock()
	s.currentPage = dataBlock
	atomic.StoreInt64(&s.pagePointer, 0)
	s.pageMu.Unlock()
}

func (s *Slab) FreeMemory(ptr unsafe.Pointer) {
	s.freeMu.Lock()
	s.freeList.Push(ptr)
	s.freeMu.Unlock()
}
