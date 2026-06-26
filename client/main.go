package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	driver "github.com/WatchJani/memCashed/client/driver"
	"github.com/bradfitz/gomemcache/memcache"
)

type Cache interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte, ttl int) error
	Close() error
}

type CustomCache struct {
	d *driver.Driver
}

func NewCustomCache() (*CustomCache, error) {
	d, err := driver.New()
	if err != nil {
		return nil, err
	}
	return &CustomCache{d: d}, nil
}

func (c *CustomCache) Get(key []byte) ([]byte, error) {
	return c.d.GetBytes(key)
}

func (c *CustomCache) Set(key []byte, value []byte, ttl int) error {
	_, err := c.d.SetBytes(key, value, ttl)
	return err
}

func (c *CustomCache) Close() error {
	c.d.Close()
	return nil
}

type MemcachedCache struct {
	c *memcache.Client
}

func NewMemcachedCache(addr string) *MemcachedCache {
	c := memcache.New(addr)
	c.Timeout = 5 * time.Second
	c.MaxIdleConns = 512
	return &MemcachedCache{c: c}
}

func (m *MemcachedCache) Get(key []byte) ([]byte, error) {
	item, err := m.c.Get(string(key))
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func (m *MemcachedCache) Set(key []byte, value []byte, ttl int) error {
	expiration := int32(0)
	if ttl > 0 {
		expiration = int32(ttl)
	}

	return m.c.Set(&memcache.Item{
		Key:        string(key),
		Value:      value,
		Expiration: expiration,
	})
}

func (m *MemcachedCache) Close() error {
	return nil
}

type Stats struct {
	totalRequests uint64
	totalErrors   uint64
	totalBytes    uint64
	latencyIndex  uint64
}

func main() {
	backend := flag.String("backend", "custom", "custom or memcached")
	memcachedAddr := flag.String("memcached", "127.0.0.1:11211", "memcached address")

	workers := flag.Int("workers", 512, "number of concurrent goroutines")
	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	warmup := flag.Duration("warmup", 10*time.Second, "warmup duration")

	getRatio := flag.Int("getratio", 70, "percentage of GET requests")
	ttl := flag.Int("ttl", -1, "ttl value")
	keyCount := flag.Int("keys", 20000, "number of keys to preload and reuse")
	valueSize := flag.Int("size", 1024, "fixed value size in bytes")

	flag.Parse()

	var cache Cache
	var err error

	switch *backend {
	case "custom":
		cache, err = NewCustomCache()
		if err != nil {
			log.Fatal(err)
		}
	case "memcached":
		cache = NewMemcachedCache(*memcachedAddr)
	default:
		log.Fatalf("unknown backend: %s", *backend)
	}

	defer cache.Close()

	value := makeValue(*valueSize)

	keys := make([]string, *keyCount)

	fmt.Println("Preloading keys...")

	for i := 0; i < *keyCount; i++ {
		key := fmt.Sprintf("key:%d", i)
		keys[i] = key

		err := cache.Set([]byte(key), value, *ttl)
		if err != nil {
			log.Fatalf("preload failed at key %d: %v", i, err)
		}
	}

	fmt.Println("Preload finished.")

	fmt.Println("Warmup started...")

	runLoad(cache, keys, value, *workers, *warmup, *getRatio, *ttl, false)

	fmt.Println("Warmup finished.")
	fmt.Println("Benchmark started...")

	maxOpsEstimate := uint64(1000000)
	latencies := make([]int64, maxOpsEstimate)

	start := time.Now()

	stats := runLoadWithLatency(
		cache,
		keys,
		value,
		*workers,
		*duration,
		*getRatio,
		*ttl,
		latencies,
	)

	elapsed := time.Since(start)

	total := atomic.LoadUint64(&stats.totalRequests)
	errors := atomic.LoadUint64(&stats.totalErrors)
	bytesTransferred := atomic.LoadUint64(&stats.totalBytes)
	latencyCount := atomic.LoadUint64(&stats.latencyIndex)

	if latencyCount > uint64(len(latencies)) {
		latencyCount = uint64(len(latencies))
	}

	usedLatencies := latencies[:latencyCount]

	sort.Slice(usedLatencies, func(i, j int) bool {
		return usedLatencies[i] < usedLatencies[j]
	})

	fmt.Println("========== LOAD TEST RESULT ==========")
	fmt.Println("Backend:", *backend)
	fmt.Println("Workers:", *workers)
	fmt.Println("Duration:", elapsed)
	fmt.Println("GET ratio:", *getRatio)
	fmt.Println("Keys:", *keyCount)
	fmt.Println("Value size:", *valueSize, "bytes")
	fmt.Println("Total requests:", total)
	fmt.Println("Errors:", errors)
	fmt.Printf("Requests/sec: %.2f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("MB/sec: %.2f\n", float64(bytesTransferred)/(1024*1024)/elapsed.Seconds())

	if len(usedLatencies) > 0 {
		fmt.Printf("p50 latency: %.3f ms\n", nsToMs(percentile(usedLatencies, 50)))
		fmt.Printf("p95 latency: %.3f ms\n", nsToMs(percentile(usedLatencies, 95)))
		fmt.Printf("p99 latency: %.3f ms\n", nsToMs(percentile(usedLatencies, 99)))
		fmt.Printf("max latency: %.3f ms\n", nsToMs(usedLatencies[len(usedLatencies)-1]))
	}

	fmt.Println("======================================")
}

func runLoad(
	cache Cache,
	keys []string,
	value []byte,
	workers int,
	duration time.Duration,
	getRatio int,
	ttl int,
	measure bool,
) {
	var wg sync.WaitGroup
	stop := time.Now().Add(duration)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for time.Now().Before(stop) {
				keyIndex := r.Intn(len(keys))
				key := []byte(keys[keyIndex])

				if r.Intn(100) < getRatio {
					_, _ = cache.Get(key)
				} else {
					_ = cache.Set(key, value, ttl)
				}
			}
		}(workerID)
	}

	wg.Wait()
}

func runLoadWithLatency(
	cache Cache,
	keys []string,
	value []byte,
	workers int,
	duration time.Duration,
	getRatio int,
	ttl int,
	latencies []int64,
) Stats {
	var stats Stats
	var wg sync.WaitGroup

	stop := time.Now().Add(duration)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for time.Now().Before(stop) {
				keyIndex := r.Intn(len(keys))
				key := []byte(keys[keyIndex])

				opStart := time.Now()

				if r.Intn(100) < getRatio {
					resp, err := cache.Get(key)
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
					} else {
						atomic.AddUint64(&stats.totalBytes, uint64(len(resp)))
					}
				} else {
					err := cache.Set(key, value, ttl)
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
					} else {
						atomic.AddUint64(&stats.totalBytes, uint64(len(value)))
					}
				}

				latency := time.Since(opStart).Nanoseconds()
				idx := atomic.AddUint64(&stats.latencyIndex, 1) - 1

				if idx < uint64(len(latencies)) {
					latencies[idx] = latency
				}

				atomic.AddUint64(&stats.totalRequests, 1)
			}
		}(workerID)
	}

	wg.Wait()

	return stats
}

func makeValue(size int) []byte {
	return bytes.Repeat([]byte("x"), size)
}

func percentile(values []int64, p int) int64 {
	if len(values) == 0 {
		return 0
	}

	index := len(values) * p / 100

	if index >= len(values) {
		index = len(values) - 1
	}

	return values[index]
}

func nsToMs(ns int64) float64 {
	return float64(ns) / 1e6
}
