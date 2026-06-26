package memory_allocator

import (
	"sync/atomic"
	"testing"

	"github.com/WatchJani/memCashed/memcached/constants"
)

const benchmarkMemory = 256 * constants.MiB

var benchmarkSizes = []int{
	8,
	16,
	31,
	64,
	127,
	255,
	511,
	1023,
	2047,
	4095,
	8191,
	16383,
	32767,
	65535,
	131071,
	262143,
	524287,
	constants.MiB,
}

func BenchmarkAllocatorAllocateBlock(b *testing.B) {
	allocator := New(benchmarkMemory)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := allocator.AllocateBlock()
		if err != nil {
			allocator = New(benchmarkMemory)

			_, err = allocator.AllocateBlock()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSlabAllocateMemory64B(b *testing.B) {
	benchmarkSingleSlab(b, 64)
}

func BenchmarkSlabAllocateMemory1KB(b *testing.B) {
	benchmarkSingleSlab(b, 1024)
}

func BenchmarkSlabAllocateMemory4KB(b *testing.B) {
	benchmarkSingleSlab(b, 4096)
}

func BenchmarkSlabAllocateMemory64KB(b *testing.B) {
	benchmarkSingleSlab(b, 65536)
}

func BenchmarkSlabAllocateMemory1MB(b *testing.B) {
	benchmarkSingleSlab(b, constants.MiB)
}

func benchmarkSingleSlab(b *testing.B, slabSize int) {
	allocator := New(benchmarkMemory)
	slab := NewSlab(slabSize, 0, allocator)

	b.ReportAllocs()
	b.SetBytes(int64(slabSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := slab.AllocateMemory()
		if err != nil {
			allocator = New(benchmarkMemory)
			slab = NewSlab(slabSize, 0, allocator)

			_, err = slab.AllocateMemory()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSlabManagerGetIndex(b *testing.B) {
	manager := newBenchmarkSlabManager()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		manager.GetIndex(benchmarkSizes[i%len(benchmarkSizes)])
	}
}

func BenchmarkSlabManagerGetSlabMixedSizes(b *testing.B) {
	manager := newBenchmarkSlabManager()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		size := benchmarkSizes[i%len(benchmarkSizes)]

		_, _, err := manager.GetSlab(size, nil)
		if err != nil {
			manager = newBenchmarkSlabManager()

			_, _, err = manager.GetSlab(size, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSlabAllocateMemoryParallel1KB(b *testing.B) {
	allocator := New(benchmarkMemory)
	slab := NewSlab(1024, 0, allocator)

	b.ReportAllocs()
	b.SetBytes(1024)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := slab.AllocateMemory()
			if err != nil {
				continue
			}
		}
	})
}

func BenchmarkSlabManagerGetSlabParallelMixedSizes(b *testing.B) {
	manager := newBenchmarkSlabManager()

	var counter uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			size := benchmarkSizes[int(i)%len(benchmarkSizes)]

			_, _, err := manager.GetSlab(size, nil)
			if err != nil {
				continue
			}
		}
	})
}

func newBenchmarkSlabManager() *SlabManager {
	allocator := New(benchmarkMemory)

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

	return NewSlabManager(slabs, 0)
}
