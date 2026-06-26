package memory_allocator

import (
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/WatchJani/memCashed/memcached/constants"
)

func TestNewTransfer(t *testing.T) {
	payload := []byte("hello")
	index := 2

	transfer := NewTransfer(payload, index, nil)

	if string(transfer.payload) != "hello" {
		t.Fatalf("expected payload hello, got %s", string(transfer.payload))
	}

	if transfer.index != index {
		t.Fatalf("expected index %d, got %d", index, transfer.index)
	}

	if transfer.conn != nil {
		t.Fatal("expected nil conn")
	}
}

func TestTLLParserNoTTL(t *testing.T) {
	ttl := TLLParser(0)

	if !ttl.IsZero() {
		t.Fatalf("expected zero time, got %v", ttl)
	}
}

func TestTLLParserWithTTL(t *testing.T) {
	before := time.Now()
	ttl := TLLParser(2)
	after := time.Now().Add(2 * time.Second)

	if ttl.IsZero() {
		t.Fatal("expected ttl time, got zero")
	}

	if ttl.Before(before) {
		t.Fatalf("ttl should not be before now, got %v", ttl)
	}

	if ttl.After(after) {
		t.Fatalf("ttl should not be after expected limit, got %v", ttl)
	}
}

func TestNewSlab(t *testing.T) {
	allocator := New(8 * constants.MiB)
	slab := NewSlab(1024, 0, allocator)

	if slab.slabSize != 1024 {
		t.Fatalf("expected slab size 1024, got %d", slab.slabSize)
	}

	if slab.Allocator == nil {
		t.Fatal("expected allocator, got nil")
	}

	if slab.IsSlabActive() {
		t.Fatal("expected slab to be inactive before allocation")
	}

	if slab.GetCurrentPage() != nil {
		t.Fatal("expected current page to be nil before allocation")
	}
}

func TestSlabAllocateMemory(t *testing.T) {
	allocator := New(8 * constants.MiB)
	slab := NewSlab(1024, 0, allocator)

	block, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("expected allocation to succeed, got error: %v", err)
	}

	if len(block) != 1024 {
		t.Fatalf("expected block size 1024, got %d", len(block))
	}

	if !slab.IsSlabActive() {
		t.Fatal("expected slab to be active after allocation")
	}

	if slab.GetCurrentPage() == nil {
		t.Fatal("expected current page to exist after allocation")
	}

	if slab.pagePointer != 1024 {
		t.Fatalf("expected pagePointer 1024, got %d", slab.pagePointer)
	}
}

func TestSlabAllocateMemoryDoesNotOverlap(t *testing.T) {
	allocator := New(8 * constants.MiB)
	slab := NewSlab(1024, 0, allocator)

	block1, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("first allocation failed: %v", err)
	}

	block2, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("second allocation failed: %v", err)
	}

	if len(block1) != 1024 {
		t.Fatalf("expected block1 size 1024, got %d", len(block1))
	}

	if len(block2) != 1024 {
		t.Fatalf("expected block2 size 1024, got %d", len(block2))
	}

	block1[0] = 11
	block1[len(block1)-1] = 22

	block2[0] = 33
	block2[len(block2)-1] = 44

	if block1[0] != 11 {
		t.Fatalf("block1 start overwritten, got %d", block1[0])
	}

	if block1[len(block1)-1] != 22 {
		t.Fatalf("block1 end overwritten, got %d", block1[len(block1)-1])
	}

	if block2[0] != 33 {
		t.Fatalf("block2 start overwritten, got %d", block2[0])
	}

	if block2[len(block2)-1] != 44 {
		t.Fatalf("block2 end overwritten, got %d", block2[len(block2)-1])
	}

	if unsafe.Pointer(&block1[0]) == unsafe.Pointer(&block2[0]) {
		t.Fatal("expected different memory addresses, got same")
	}
}

func TestSlabAllocateMemoryCreatesNewPage(t *testing.T) {
	allocator := New(4 * constants.MiB)
	slab := NewSlab(constants.MiB/2, 0, allocator)

	block1, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("first allocation failed: %v", err)
	}

	block2, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("second allocation failed: %v", err)
	}

	block3, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("third allocation failed: %v", err)
	}

	if len(block1) != constants.MiB/2 {
		t.Fatalf("expected block1 size %d, got %d", constants.MiB/2, len(block1))
	}

	if len(block2) != constants.MiB/2 {
		t.Fatalf("expected block2 size %d, got %d", constants.MiB/2, len(block2))
	}

	if len(block3) != constants.MiB/2 {
		t.Fatalf("expected block3 size %d, got %d", constants.MiB/2, len(block3))
	}

	if allocator.GetNext() != 2*constants.MiB {
		t.Fatalf("expected allocator next %d, got %d", 2*constants.MiB, allocator.GetNext())
	}

	if slab.pagePointer != constants.MiB/2 {
		t.Fatalf("expected pagePointer %d after new page, got %d", constants.MiB/2, slab.pagePointer)
	}
}

func TestSlabAllocateMemoryReusesFreeList(t *testing.T) {
	allocator := New(4 * constants.MiB)
	slab := NewSlab(1024, 0, allocator)

	block1, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("first allocation failed: %v", err)
	}

	ptr := unsafe.Pointer(&block1[0])
	slab.freeList.Push(ptr)

	block2, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("second allocation failed: %v", err)
	}

	if unsafe.Pointer(&block2[0]) != ptr {
		t.Fatal("expected allocator to reuse block from freeList")
	}
}

func TestSlabAllocateMemoryAllCommonSizes(t *testing.T) {
	allocator := New(512 * constants.MiB)

	sizes := []int{
		8,
		16,
		32,
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
		constants.MiB,
	}

	for _, size := range sizes {
		t.Run("size", func(t *testing.T) {
			slab := NewSlab(size, 0, allocator)

			for i := 0; i < 100; i++ {
				block, err := slab.AllocateMemory()
				if err != nil {
					t.Fatalf("allocation failed for slab size %d: %v", size, err)
				}

				if len(block) != size {
					t.Fatalf("expected block size %d, got %d", size, len(block))
				}

				block[0] = byte(i)
				block[len(block)-1] = byte(i + 1)
			}
		})
	}
}

func TestSlabAllocateMemoryConcurrent(t *testing.T) {
	allocator := New(256 * constants.MiB)
	slab := NewSlab(1024, 0, allocator)

	const goroutines = 100
	const allocationsPerGoroutine = 1000

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*allocationsPerGoroutine)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < allocationsPerGoroutine; j++ {
				block, err := slab.AllocateMemory()
				if err != nil {
					errCh <- err
					return
				}

				if len(block) != 1024 {
					t.Errorf("expected block size 1024, got %d", len(block))
					return
				}

				block[0] = byte(workerID)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("expected no errors, got %v", err)
		}
	}
}

func TestSlabAllocateMemoryOutOfSpace(t *testing.T) {
	allocator := New(constants.MiB)
	slab := NewSlab(constants.MiB, 0, allocator)

	_, err := slab.AllocateMemory()
	if err != nil {
		t.Fatalf("first allocation should succeed, got %v", err)
	}

	_, err = slab.AllocateMemory()
	if err == nil {
		t.Fatal("expected out of space error, got nil")
	}

	if err != constants.ErrNotEnoughSpace {
		t.Fatalf("expected ErrNotEnoughSpace, got %v", err)
	}
}

func TestNewSlabManager(t *testing.T) {
	allocator := New(64 * constants.MiB)

	slabs := []Slab{
		NewSlab(8, 0, allocator),
		NewSlab(16, 0, allocator),
		NewSlab(32, 0, allocator),
		NewSlab(64, 0, allocator),
		NewSlab(128, 0, allocator),
		NewSlab(256, 0, allocator),
		NewSlab(512, 0, allocator),
		NewSlab(1024, 0, allocator),
	}

	manager := NewSlabManager(slabs, 4)

	if manager == nil {
		t.Fatal("expected manager, got nil")
	}

	if len(manager.slabs) != len(slabs) {
		t.Fatalf("expected %d slabs, got %d", len(slabs), len(manager.slabs))
	}

	if len(manager.lru) != len(slabs) {
		t.Fatalf("expected %d lru lists, got %d", len(slabs), len(manager.lru))
	}

	if manager.JobCh == nil {
		t.Fatal("expected JobCh, got nil")
	}

	if cap(manager.JobCh) != 65536 {
		t.Fatalf("expected JobCh capacity 65536, got %d", cap(manager.JobCh))
	}
}

func TestSlabManagerGetIndex(t *testing.T) {
	allocator := New(64 * constants.MiB)

	slabs := []Slab{
		NewSlab(8, 0, allocator),
		NewSlab(16, 0, allocator),
		NewSlab(32, 0, allocator),
		NewSlab(64, 0, allocator),
		NewSlab(128, 0, allocator),
		NewSlab(256, 0, allocator),
		NewSlab(512, 0, allocator),
		NewSlab(1024, 0, allocator),
	}

	manager := NewSlabManager(slabs, 0)

	tests := []struct {
		dataSize          int
		expectedSlabIndex int
		expectedChunkSize int
	}{
		{1, 0, 8},
		{8, 0, 8},
		{9, 1, 16},
		{16, 1, 16},
		{17, 2, 32},
		{31, 2, 32},
		{32, 2, 32},
		{33, 3, 64},
		{64, 3, 64},
		{65, 4, 128},
		{128, 4, 128},
		{129, 5, 256},
		{256, 5, 256},
		{257, 6, 512},
		{512, 6, 512},
		{513, 7, 1024},
		{1024, 7, 1024},
	}

	for _, tt := range tests {
		index, chunkSize := manager.GetIndex(tt.dataSize)

		if index != tt.expectedSlabIndex {
			t.Fatalf("dataSize=%d expected index %d, got %d", tt.dataSize, tt.expectedSlabIndex, index)
		}

		if chunkSize != tt.expectedChunkSize {
			t.Fatalf("dataSize=%d expected chunk size %d, got %d", tt.dataSize, tt.expectedChunkSize, chunkSize)
		}
	}
}

func TestSlabManagerGetSlabIndex(t *testing.T) {
	allocator := New(64 * constants.MiB)

	slabs := []Slab{
		NewSlab(8, 0, allocator),
		NewSlab(16, 0, allocator),
		NewSlab(32, 0, allocator),
	}

	manager := NewSlabManager(slabs, 0)

	slab := manager.GetSlabIndex(1)

	if slab == nil {
		t.Fatal("expected slab, got nil")
	}

	if slab.slabSize != 16 {
		t.Fatalf("expected slab size 16, got %d", slab.slabSize)
	}
}

func TestSlabManagerGetLRUIndex(t *testing.T) {
	allocator := New(64 * constants.MiB)

	slabs := []Slab{
		NewSlab(8, 0, allocator),
		NewSlab(16, 0, allocator),
		NewSlab(32, 0, allocator),
	}

	manager := NewSlabManager(slabs, 0)

	lru := manager.GetLRUIndex(1)

	if lru == nil {
		t.Fatal("expected lru, got nil")
	}
}

func TestSlabManagerChoseSlab(t *testing.T) {
	allocator := New(64 * constants.MiB)

	slabs := []Slab{
		NewSlab(8, 0, allocator),
		NewSlab(16, 0, allocator),
		NewSlab(32, 0, allocator),
	}

	manager := NewSlabManager(slabs, 0)

	slab := manager.ChoseSlab(2)

	if slab == nil {
		t.Fatal("expected slab, got nil")
	}

	if slab.slabSize != 32 {
		t.Fatalf("expected slab size 32, got %d", slab.slabSize)
	}
}

func TestSlabManagerGetSlab(t *testing.T) {
	allocator := New(64 * constants.MiB)

	slabs := []Slab{
		NewSlab(8, 0, allocator),
		NewSlab(16, 0, allocator),
		NewSlab(32, 0, allocator),
		NewSlab(64, 0, allocator),
		NewSlab(128, 0, allocator),
		NewSlab(256, 0, allocator),
		NewSlab(512, 0, allocator),
		NewSlab(1024, 0, allocator),
	}

	manager := NewSlabManager(slabs, 0)

	block, index, err := manager.GetSlab(100, nil)
	if err != nil {
		t.Fatalf("expected GetSlab to succeed, got %v", err)
	}

	if index != 4 {
		t.Fatalf("expected slab index 4, got %d", index)
	}

	if len(block) != 128 {
		t.Fatalf("expected block size 128, got %d", len(block))
	}
}

func TestSlabManagerGetSlabAllSizes(t *testing.T) {
	allocator := New(512 * constants.MiB)

	slabs := []Slab{
		NewSlab(8, 0, allocator),
		NewSlab(16, 0, allocator),
		NewSlab(32, 0, allocator),
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
		NewSlab(131072, 0, allocator),
		NewSlab(262144, 0, allocator),
		NewSlab(524288, 0, allocator),
		NewSlab(constants.MiB, 0, allocator),
	}

	manager := NewSlabManager(slabs, 0)

	payloadSizes := []int{
		1,
		8,
		9,
		16,
		17,
		32,
		33,
		64,
		65,
		128,
		129,
		256,
		257,
		512,
		513,
		1024,
		1025,
		2048,
		2049,
		4096,
		4097,
		8192,
		8193,
		16384,
		16385,
		32768,
		32769,
		65536,
		65537,
		131072,
		131073,
		262144,
		262145,
		524288,
		524289,
		constants.MiB,
	}

	for _, payloadSize := range payloadSizes {
		block, _, err := manager.GetSlab(payloadSize, nil)
		if err != nil {
			t.Fatalf("payloadSize=%d expected success, got %v", payloadSize, err)
		}

		if len(block) < payloadSize {
			t.Fatalf("payloadSize=%d got too small block %d", payloadSize, len(block))
		}
	}
}