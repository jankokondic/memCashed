package memory_allocator

import (
	"sync"
	"testing"

	"github.com/WatchJani/memCashed/memcached/constants"
)

func TestNewAllocator(t *testing.T) {
	capacity := 10 * constants.MiB

	allocator := New(capacity)

	if allocator == nil {
		t.Fatal("expected allocator, got nil")
	}

	if len(allocator.memory) != capacity {
		t.Fatalf("expected memory size %d, got %d", capacity, len(allocator.memory))
	}

	if allocator.GetNext() != 0 {
		t.Fatalf("expected next to be 0, got %d", allocator.GetNext())
	}
}

func TestIsEnoughSpace(t *testing.T) {
	tests := []struct {
		name     string
		end      int
		length   int
		expected bool
	}{
		{
			name:     "has enough space",
			end:      5,
			length:   10,
			expected: true,
		},
		{
			name:     "exact fit",
			end:      10,
			length:   10,
			expected: true,
		},
		{
			name:     "not enough space",
			end:      11,
			length:   10,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsEnoughSpace(tt.end, tt.length)

			if got != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestAllocateBlock(t *testing.T) {
	capacity := 3 * constants.MiB

	allocator := New(capacity)

	block1, err := allocator.AllocateBlock()
	if err != nil {
		t.Fatalf("expected first allocation to succeed, got error: %v", err)
	}

	if len(block1) != constants.MiB {
		t.Fatalf("expected block size %d, got %d", constants.MiB, len(block1))
	}

	if allocator.GetNext() != constants.MiB {
		t.Fatalf("expected next %d, got %d", constants.MiB, allocator.GetNext())
	}

	block2, err := allocator.AllocateBlock()
	if err != nil {
		t.Fatalf("expected second allocation to succeed, got error: %v", err)
	}

	if len(block2) != constants.MiB {
		t.Fatalf("expected block size %d, got %d", constants.MiB, len(block2))
	}

	if allocator.GetNext() != 2*constants.MiB {
		t.Fatalf("expected next %d, got %d", 2*constants.MiB, allocator.GetNext())
	}

	block3, err := allocator.AllocateBlock()
	if err != nil {
		t.Fatalf("expected third allocation to succeed, got error: %v", err)
	}

	if len(block3) != constants.MiB {
		t.Fatalf("expected block size %d, got %d", constants.MiB, len(block3))
	}

	if allocator.GetNext() != 3*constants.MiB {
		t.Fatalf("expected next %d, got %d", 3*constants.MiB, allocator.GetNext())
	}

	_, err = allocator.AllocateBlock()
	if err == nil {
		t.Fatal("expected error when allocator is full, got nil")
	}

	if err != constants.ErrNotEnoughSpace {
		t.Fatalf("expected ErrNotEnoughSpace, got %v", err)
	}
}

func TestAllocateBlockDoesNotOverlap(t *testing.T) {
	capacity := 2 * constants.MiB

	allocator := New(capacity)

	block1, err := allocator.AllocateBlock()
	if err != nil {
		t.Fatalf("expected first allocation to succeed, got error: %v", err)
	}

	block2, err := allocator.AllocateBlock()
	if err != nil {
		t.Fatalf("expected second allocation to succeed, got error: %v", err)
	}

	block1[0] = 1
	block1[len(block1)-1] = 2

	block2[0] = 3
	block2[len(block2)-1] = 4

	if block1[0] != 1 {
		t.Fatalf("block1 start was overwritten, got %d", block1[0])
	}

	if block1[len(block1)-1] != 2 {
		t.Fatalf("block1 end was overwritten, got %d", block1[len(block1)-1])
	}

	if block2[0] != 3 {
		t.Fatalf("block2 start was overwritten, got %d", block2[0])
	}

	if block2[len(block2)-1] != 4 {
		t.Fatalf("block2 end was overwritten, got %d", block2[len(block2)-1])
	}
}

func TestAllocateBlockConcurrent(t *testing.T) {
	numberOfBlocks := 100
	capacity := numberOfBlocks * constants.MiB

	allocator := New(capacity)

	var wg sync.WaitGroup
	errCh := make(chan error, numberOfBlocks)

	for i := 0; i < numberOfBlocks; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			block, err := allocator.AllocateBlock()
			if err != nil {
				errCh <- err
				return
			}

			if len(block) != constants.MiB {
				t.Errorf("expected block size %d, got %d", constants.MiB, len(block))
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("expected no allocation errors, got %v", err)
		}
	}

	expectedNext := numberOfBlocks * constants.MiB
	if allocator.GetNext() != expectedNext {
		t.Fatalf("expected next %d, got %d", expectedNext, allocator.GetNext())
	}
}

func TestAllocateBlockConcurrentTooManyRequests(t *testing.T) {
	numberOfBlocks := 10
	numberOfRequests := 50
	capacity := numberOfBlocks * constants.MiB

	allocator := New(capacity)

	var wg sync.WaitGroup
	var successCount int
	var errorCount int
	var mu sync.Mutex

	for i := 0; i < numberOfRequests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := allocator.AllocateBlock()

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errorCount++
				return
			}

			successCount++
		}()
	}

	wg.Wait()

	if successCount != numberOfBlocks {
		t.Fatalf("expected %d successful allocations, got %d", numberOfBlocks, successCount)
	}

	expectedErrors := numberOfRequests - numberOfBlocks
	if errorCount != expectedErrors {
		t.Fatalf("expected %d errors, got %d", expectedErrors, errorCount)
	}

	expectedNext := numberOfBlocks * constants.MiB
	if allocator.GetNext() != expectedNext {
		t.Fatalf("expected next %d, got %d", expectedNext, allocator.GetNext())
	}
}