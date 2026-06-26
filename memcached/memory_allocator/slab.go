package memory_allocator

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
	"unsafe"

	"github.com/WatchJani/memCashed/memcached/constants"
	"github.com/WatchJani/memCashed/memcached/link_list"
	"github.com/WatchJani/memCashed/memcached/stack"
)

// SlabManager manages slabs, LRU (Least Recently Used) caches, and memory allocation.
type SlabManager struct {
	slabs        []Slab          // Slabs for memory allocation
	lru          []link_list.DLL // Least Recently Used (LRU) cache for each slab
	sync.RWMutex                 // Mutex to protect concurrent access to shared data
	store        sync.Map        // Store to hold key-value pairs for cache management
	JobCh        chan Transfer   // Channel to receive transfer jobs for processing
}

// Transfer represents a data payload and connection information for a transfer task.
type Transfer struct {
	payload []byte    // Data payload
	conn    io.Writer // Network connection
	index   int       // Index of the slab category
}

// Key represents a stored object with its field, TTL (Time-To-Live), and a pointer to its node in the LRU list.
type Key struct {
	field     []byte
	ttl       time.Time
	pointer   *link_list.Node
	slabIndex int
}

// NewTransfer creates a new Transfer object with the specified payload, index, and connection.
func NewTransfer(payload []byte, index int, conn io.Writer) Transfer { //Connection -> io.writer
	return Transfer{
		payload: payload,
		conn:    conn,
		index:   index,
	}
}

// FreeSpace frees space in the slab's LRU cache by removing the least recently used node.
func (s *SlabManager) FreeSpace(index, slabSize int) ([]byte, string) {
	s.Lock()
	defer s.Unlock()

	lastNode := s.lru[index].LastNode() // Get the last (least recently used) node

	s.lru[index].Delete(lastNode) // Delete the last node in the LRU cache

	// Get free space from LRU after deleting the node
	return s.lru[index].GetLRUFreeSpace(lastNode, slabSize), lastNode.GetKey()
}

// GetSlabIndex returns the slab at the specified index.
func (s *SlabManager) GetSlabIndex(index int) *Slab {
	return &s.slabs[index]
}

// GetLRUIndex returns the LRU cache at the specified index.
func (s *SlabManager) GetLRUIndex(index int) *link_list.DLL {
	return &s.lru[index]
}

// NewSlabManager creates a new SlabManager with the provided slabs and starts worker goroutines.
func NewSlabManager(slabs []Slab, numberOfWorker int) *SlabManager {
	sm := &SlabManager{
		slabs: slabs,
		lru:   make([]link_list.DLL, len(slabs)), // Initialize LRU for each slab
		JobCh: make(chan Transfer, 65536),        // Channel for receiving transfer jobs
	}

	// Start a worker goroutine of numberOfWorker
	for range numberOfWorker {
		go sm.Worker()
	}

	return sm
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

	s.Lock()

	lastNode := s.lru[slabIndex].LastNode()
	if lastNode == nil {
		s.Unlock()
		return nil, slabIndex, fmt.Errorf("there is not enough space and LRU is empty")
	}

	s.lru[slabIndex].Delete(lastNode)
	slabBlock = s.lru[slabIndex].GetLRUFreeSpace(lastNode, chunkSize)
	key := lastNode.GetKey()

	s.Unlock()

	s.store.Delete(key)

	return slabBlock, slabIndex, nil
}

// TLLParser converts a TTL value into a time.Time object.
func TLLParser(ttl uint32) time.Time {
	if ttl > 0 {
		return time.Now().Add(time.Duration(ttl) * time.Second) // Add TTL to the current time
	}

	return time.Time{} // Return an empty time if TTL is 0
}

// GetIndex performs a binary search to find the appropriate slab index based on the data size.
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

// ChoseSlab returns the slab at the specified index.
func (s *SlabManager) ChoseSlab(index int) *Slab {
	return &s.slabs[index]
}

// Slab represents a memory slab used for allocation.
type Slab struct {
	slabSize     int                         // Size of the slab
	freeList     stack.Stack[unsafe.Pointer] // Stack of free blocks in the slab
	currentPage  []byte                      // Current memory page in the slab
	pagePointer  int                         // Pointer to the current position in the slab
	sync.RWMutex                             // Mutex to protect access to the slab
	*Allocator                               // Memory allocator associated with the slab
}

// IsSlabActive checks if the slab has an active memory page.
func (s *Slab) IsSlabActive() bool {
	return s.currentPage != nil
}

// GetCurrentPage returns the current page of the slab.
func (s *Slab) GetCurrentPage() []byte {
	return s.currentPage
}

// NewSlab creates a new Slab with the specified size and allocator.
func NewSlab(slabSize, maxMemoryAllocate int, allocator *Allocator) Slab {
	return Slab{
		slabSize:  slabSize,
		freeList:  stack.New[unsafe.Pointer](10),
		Allocator: allocator,
	}
}

// AllocateMemory allocates memory for the slab, either by reusing a free block or allocating a new page.
func (s *Slab) AllocateMemory() ([]byte, error) {
	s.Lock()
	defer s.Unlock()

	if !s.freeList.IsEmpty() {
		ptr, err := s.freeList.Pop()
		if err != nil {
			return nil, err
		}

		return unsafe.Slice((*byte)(ptr), s.slabSize), nil
	}

	start := s.pagePointer
	end := start + s.slabSize

	if s.currentPage == nil || !IsEnoughSpace(end, len(s.currentPage)) {
		block, err := s.AllocateBlock()
		if err != nil {
			return nil, err
		}

		s.UpdatePage(block)

		start = 0
		end = s.slabSize
	}

	s.pagePointer = end

	return s.currentPage[start:end], nil
}

func (s *Slab) UpdatePage(dataBlock []byte) {
	s.currentPage = dataBlock
	s.pagePointer = 0
}
