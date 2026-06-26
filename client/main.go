package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
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

func NewCustomCache(mode driver.Mode) (*CustomCache, error) {
	d, err := driver.NewWithMode(mode)
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
	return c.d.Close()
}

type MemcachedCache struct {
	c *memcache.Client
}

func NewMemcachedCache(addr string) *MemcachedCache {
	c := memcache.New(addr)
	c.Timeout = 5 * time.Second
	c.MaxIdleConns = 1024

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

type pendingCustomReq struct {
	ch      chan driver.Result
	isGet   bool
	startNs int64
}

type pendingMemcachedReq struct {
	isGet bool
}

func main() {
	backend := flag.String(
		"backend",
		"custom",
		"custom, custom-sync, custom-pipeline, memcached, memcached-sync, memcached-pipeline, memcached-async, memcached-pipeline-noreply",
	)
	memcachedAddr := flag.String("memcached", "127.0.0.1:11211", "memcached address")

	workers := flag.Int("workers", 64, "number of concurrent workers")
	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	warmup := flag.Duration("warmup", 5*time.Second, "warmup duration")

	getRatio := flag.Int("getratio", 70, "percentage of GET requests")
	ttl := flag.Int("ttl", -1, "ttl value")
	keyCount := flag.Int("keys", 2000, "number of keys")
	valueSize := flag.Int("size", 1024, "fixed value size in bytes")
	pipeline := flag.Int("pipeline", 16, "pipeline depth for pipeline modes")

	flag.Parse()

	value := makeValue(*valueSize)

	keys := make([]string, *keyCount)
	for i := 0; i < *keyCount; i++ {
		keys[i] = fmt.Sprintf("key:%d", i)
	}

	switch *backend {
	case "custom-pipeline":
		runCustomPipelineBenchmark(keys, value, *workers, *duration, *warmup, *getRatio, *ttl, *pipeline, *backend)
		return

	case "memcached-pipeline", "memcached-async":
		runMemcachedPipelineBenchmark(*memcachedAddr, keys, value, *workers, *duration, *warmup, *getRatio, *ttl, *pipeline, *backend, false)
		return

	case "memcached-pipeline-noreply":
		runMemcachedPipelineBenchmark(*memcachedAddr, keys, value, *workers, *duration, *warmup, *getRatio, *ttl, *pipeline, *backend, true)
		return
	}

	cache, err := newSyncCache(*backend, *memcachedAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close()

	fmt.Println("Preloading keys...")

	for i := 0; i < *keyCount; i++ {
		err := cache.Set([]byte(keys[i]), value, *ttl)
		if err != nil {
			log.Fatalf("preload failed at key %d: %v", i, err)
		}
	}

	fmt.Println("Preload finished.")
	fmt.Println("Warmup started...")

	_ = runSyncLoad(cache, keys, value, *workers, *warmup, *getRatio, *ttl, nil)

	fmt.Println("Warmup finished.")
	fmt.Println("Benchmark started...")

	latencies := make([]int64, 1000000)

	start := time.Now()
	stats := runSyncLoad(cache, keys, value, *workers, *duration, *getRatio, *ttl, latencies)
	elapsed := time.Since(start)

	printResult(*backend, *workers, elapsed, *getRatio, *keyCount, *valueSize, stats, latencies, *pipeline)
}

func newSyncCache(backend string, memcachedAddr string) (Cache, error) {
	switch backend {
	case "custom", "custom-sync":
		return NewCustomCache(driver.ModeSync)

	case "memcached", "memcached-sync":
		return NewMemcachedCache(memcachedAddr), nil

	default:
		return nil, fmt.Errorf("unknown backend: %s", backend)
	}
}

func runSyncLoad(
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
				key := []byte(keys[r.Intn(len(keys))])

				var opStart time.Time
				if latencies != nil {
					opStart = time.Now()
				}

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

				if latencies != nil {
					latency := time.Since(opStart).Nanoseconds()
					idx := atomic.AddUint64(&stats.latencyIndex, 1) - 1
					if idx < uint64(len(latencies)) {
						latencies[idx] = latency
					}
				}

				atomic.AddUint64(&stats.totalRequests, 1)
			}
		}(workerID)
	}

	wg.Wait()

	return stats
}

func runCustomPipelineBenchmark(
	keys []string,
	value []byte,
	workers int,
	duration time.Duration,
	warmup time.Duration,
	getRatio int,
	ttl int,
	pipeline int,
	backend string,
) {
	cache, err := NewCustomCache(driver.ModePipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close()

	fmt.Println("Preloading keys...")

	for i := 0; i < len(keys); i++ {
		err := cache.Set([]byte(keys[i]), value, ttl)
		if err != nil {
			log.Fatalf("preload failed at key %d: %v", i, err)
		}
	}

	fmt.Println("Preload finished.")
	fmt.Println("Warmup started...")

	_ = runCustomPipeline(cache.d, keys, value, workers, warmup, getRatio, ttl, pipeline, nil)

	fmt.Println("Warmup finished.")
	fmt.Println("Benchmark started...")

	latencies := make([]int64, 1000000)

	start := time.Now()
	stats := runCustomPipeline(cache.d, keys, value, workers, duration, getRatio, ttl, pipeline, latencies)
	elapsed := time.Since(start)

	printResult(backend, workers, elapsed, getRatio, len(keys), len(value), stats, latencies, pipeline)
}

func runCustomPipeline(
	d *driver.Driver,
	keys []string,
	value []byte,
	workers int,
	duration time.Duration,
	getRatio int,
	ttl int,
	pipeline int,
	latencies []int64,
) Stats {
	var stats Stats
	var wg sync.WaitGroup

	if pipeline <= 0 {
		pipeline = 16
	}

	stop := time.Now().Add(duration)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			pending := make([]pendingCustomReq, 0, pipeline)

			for time.Now().Before(stop) {
				for len(pending) < pipeline && time.Now().Before(stop) {
					key := []byte(keys[r.Intn(len(keys))])
					startNs := time.Now().UnixNano()

					if r.Intn(100) < getRatio {
						ch, err := d.GetAsync(key)
						if err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
							continue
						}

						pending = append(pending, pendingCustomReq{
							ch:      ch,
							isGet:   true,
							startNs: startNs,
						})
					} else {
						ch, err := d.SetAsync(key, value, ttl)
						if err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
							continue
						}

						pending = append(pending, pendingCustomReq{
							ch:      ch,
							isGet:   false,
							startNs: startNs,
						})
					}
				}

				for i := 0; i < len(pending); i++ {
					res := <-pending[i].ch

					if res.Err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
					} else if pending[i].isGet {
						atomic.AddUint64(&stats.totalBytes, uint64(len(res.Data)))
					} else {
						atomic.AddUint64(&stats.totalBytes, uint64(len(value)))
					}

					if latencies != nil {
						latency := time.Now().UnixNano() - pending[i].startNs
						idx := atomic.AddUint64(&stats.latencyIndex, 1) - 1

						if idx < uint64(len(latencies)) {
							latencies[idx] = latency
						}
					}

					atomic.AddUint64(&stats.totalRequests, 1)
				}

				pending = pending[:0]
			}
		}(workerID)
	}

	wg.Wait()

	return stats
}

func runMemcachedPipelineBenchmark(
	addr string,
	keys []string,
	value []byte,
	workers int,
	duration time.Duration,
	warmup time.Duration,
	getRatio int,
	ttl int,
	pipeline int,
	backend string,
	noReplySets bool,
) {
	fmt.Println("Preloading memcached...")

	if err := preloadMemcached(addr, keys, value, ttl); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Preload finished.")
	fmt.Println("Warmup started...")

	_ = runMemcachedPipeline(addr, keys, value, workers, warmup, getRatio, ttl, pipeline, noReplySets)

	fmt.Println("Warmup finished.")
	fmt.Println("Benchmark started...")

	start := time.Now()
	stats := runMemcachedPipeline(addr, keys, value, workers, duration, getRatio, ttl, pipeline, noReplySets)
	elapsed := time.Since(start)

	printResult(backend, workers, elapsed, getRatio, len(keys), len(value), stats, nil, pipeline)
}

func preloadMemcached(addr string, keys []string, value []byte, ttl int) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	configureConn(conn)

	reader := bufio.NewReaderSize(conn, 1024*1024)
	writer := bufio.NewWriterSize(conn, 1024*1024)

	memTTL := ttl
	if memTTL < 0 {
		memTTL = 0
	}

	for _, key := range keys {
		_, err := fmt.Fprintf(writer, "set %s 0 %d %d\r\n", key, memTTL, len(value))
		if err != nil {
			return err
		}

		if _, err = writer.Write(value); err != nil {
			return err
		}

		if _, err = writer.WriteString("\r\n"); err != nil {
			return err
		}

		if err = writer.Flush(); err != nil {
			return err
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		if line != "STORED\r\n" {
			return fmt.Errorf("preload failed for %s: %q", key, line)
		}
	}

	return nil
}

func runMemcachedPipeline(
	addr string,
	keys []string,
	value []byte,
	workers int,
	duration time.Duration,
	getRatio int,
	ttl int,
	pipeline int,
	noReplySets bool,
) Stats {
	var stats Stats
	var wg sync.WaitGroup

	if pipeline <= 0 {
		pipeline = 16
	}

	stop := time.Now().Add(duration)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				atomic.AddUint64(&stats.totalErrors, 1)
				return
			}
			defer conn.Close()

			configureConn(conn)

			reader := bufio.NewReaderSize(conn, 1024*1024)
			writer := bufio.NewWriterSize(conn, 1024*1024)

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			pending := make([]pendingMemcachedReq, 0, pipeline)

			memTTL := ttl
			if memTTL < 0 {
				memTTL = 0
			}

			for time.Now().Before(stop) {
				for len(pending) < pipeline && time.Now().Before(stop) {
					key := keys[r.Intn(len(keys))]

					if r.Intn(100) < getRatio {
						if _, err := writer.WriteString("get "); err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
							return
						}

						if _, err := writer.WriteString(key); err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
							return
						}

						if _, err := writer.WriteString("\r\n"); err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
							return
						}

						pending = append(pending, pendingMemcachedReq{isGet: true})
					} else {
						if noReplySets {
							_, err := fmt.Fprintf(writer, "set %s 0 %d %d noreply\r\n", key, memTTL, len(value))
							if err != nil {
								atomic.AddUint64(&stats.totalErrors, 1)
								return
							}

							if _, err = writer.Write(value); err != nil {
								atomic.AddUint64(&stats.totalErrors, 1)
								return
							}

							if _, err = writer.WriteString("\r\n"); err != nil {
								atomic.AddUint64(&stats.totalErrors, 1)
								return
							}

							atomic.AddUint64(&stats.totalBytes, uint64(len(value)))
							atomic.AddUint64(&stats.totalRequests, 1)
						} else {
							_, err := fmt.Fprintf(writer, "set %s 0 %d %d\r\n", key, memTTL, len(value))
							if err != nil {
								atomic.AddUint64(&stats.totalErrors, 1)
								return
							}

							if _, err = writer.Write(value); err != nil {
								atomic.AddUint64(&stats.totalErrors, 1)
								return
							}

							if _, err = writer.WriteString("\r\n"); err != nil {
								atomic.AddUint64(&stats.totalErrors, 1)
								return
							}

							pending = append(pending, pendingMemcachedReq{isGet: false})
						}
					}
				}

				if err := writer.Flush(); err != nil {
					atomic.AddUint64(&stats.totalErrors, 1)
					return
				}

				for i := 0; i < len(pending); i++ {
					if pending[i].isGet {
						n, err := readOneGETResponse(reader)
						if err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
						} else {
							atomic.AddUint64(&stats.totalBytes, uint64(n))
						}
					} else {
						if err := readOneSetResponse(reader); err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
						} else {
							atomic.AddUint64(&stats.totalBytes, uint64(len(value)))
						}
					}

					atomic.AddUint64(&stats.totalRequests, 1)
				}

				pending = pending[:0]
			}

			_ = writer.Flush()
		}(workerID)
	}

	wg.Wait()

	return stats
}

func readOneSetResponse(reader *bufio.Reader) error {
	line, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	if line != "STORED\r\n" {
		return fmt.Errorf("expected STORED, got %q", line)
	}

	return nil
}

func configureConn(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(1024 * 1024)
	_ = tcpConn.SetWriteBuffer(1024 * 1024)
}

func readOneGETResponse(reader *bufio.Reader) (int, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	if line == "END\r\n" {
		return 0, nil
	}

	if !strings.HasPrefix(line, "VALUE ") {
		return 0, fmt.Errorf("unexpected response: %q", line)
	}

	parts := strings.Split(line, " ")
	if len(parts) < 4 {
		return 0, fmt.Errorf("invalid VALUE line: %q", line)
	}

	sizeText := strings.TrimSpace(parts[3])
	size, err := strconv.Atoi(sizeText)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, size+2)

	if _, err = io.ReadFull(reader, buf); err != nil {
		return 0, err
	}

	endLine, err := reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	if endLine != "END\r\n" {
		return 0, fmt.Errorf("expected END, got %q", endLine)
	}

	return size, nil
}

func printResult(
	backend string,
	workers int,
	elapsed time.Duration,
	getRatio int,
	keyCount int,
	valueSize int,
	stats Stats,
	latencies []int64,
	pipeline int,
) {
	total := atomic.LoadUint64(&stats.totalRequests)
	errors := atomic.LoadUint64(&stats.totalErrors)
	bytesTransferred := atomic.LoadUint64(&stats.totalBytes)

	fmt.Println("========== LOAD TEST RESULT ==========")
	fmt.Println("Backend:", backend)
	fmt.Println("Workers:", workers)
	fmt.Println("Duration:", elapsed)
	fmt.Println("GET ratio:", getRatio)
	fmt.Println("Pipeline:", pipeline)
	fmt.Println("Keys:", keyCount)
	fmt.Println("Value size:", valueSize, "bytes")
	fmt.Println("Total requests:", total)
	fmt.Println("Errors:", errors)
	fmt.Printf("Requests/sec: %.2f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("MB/sec: %.2f\n", float64(bytesTransferred)/(1024*1024)/elapsed.Seconds())

	if latencies != nil {
		latencyCount := atomic.LoadUint64(&stats.latencyIndex)

		if latencyCount > uint64(len(latencies)) {
			latencyCount = uint64(len(latencies))
		}

		usedLatencies := latencies[:latencyCount]

		sort.Slice(usedLatencies, func(i, j int) bool {
			return usedLatencies[i] < usedLatencies[j]
		})

		if len(usedLatencies) > 0 {
			fmt.Printf("p50 latency: %.3f ms\n", nsToMs(percentile(usedLatencies, 50)))
			fmt.Printf("p95 latency: %.3f ms\n", nsToMs(percentile(usedLatencies, 95)))
			fmt.Printf("p99 latency: %.3f ms\n", nsToMs(percentile(usedLatencies, 99)))
			fmt.Printf("max latency: %.3f ms\n", nsToMs(usedLatencies[len(usedLatencies)-1]))
		}
	}

	fmt.Println("======================================")
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
