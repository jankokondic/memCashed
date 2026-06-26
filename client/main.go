package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	driver "github.com/WatchJani/memCashed/client/driver"
)

type Stats struct {
	totalRequests uint64
	totalErrors   uint64
	totalBytes    uint64
}

func main() {
	workers := flag.Int("workers", 512, "number of concurrent goroutines")
	requestsPerWorker := flag.Int("requests", 100000, "requests per worker")
	getRatio := flag.Int("getratio", 70, "percentage of GET requests")
	ttl := flag.Int("ttl", -1, "ttl value")
	keyCount := flag.Int("keys", 20000, "number of keys to preload and reuse")
	maxValueSize := flag.Int("maxsize", 65536, "maximum value size in bytes")
	flag.Parse()

	valueSizes := []int{
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
		1048576,
	}

	filteredSizes := make([]int, 0, len(valueSizes))
	for _, size := range valueSizes {
		if size <= *maxValueSize {
			filteredSizes = append(filteredSizes, size)
		}
	}

	if len(filteredSizes) == 0 {
		log.Fatal("no value sizes available")
	}

	clientDriver, err := driver.New()
	if err != nil {
		log.Fatal(err)
	}
	defer clientDriver.Close()

	keys := make([]string, *keyCount)
	values := make([][]byte, len(filteredSizes))

	for i, size := range filteredSizes {
		values[i] = makeValue(size)
	}

	fmt.Println("Preloading keys...")

	for i := 0; i < *keyCount; i++ {
		sizeIndex := i % len(values)
		keys[i] = fmt.Sprintf("key:%d", i)

		_, err := clientDriver.SetBytes([]byte(keys[i]), values[sizeIndex], *ttl)
		if err != nil {
			log.Fatalf("preload failed at key %d: %v", i, err)
		}
	}

	fmt.Println("Preload finished.")
	fmt.Println("Starting load test...")

	var stats Stats
	var wg sync.WaitGroup

	start := time.Now()

	for workerID := 0; workerID < *workers; workerID++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for i := 0; i < *requestsPerWorker; i++ {
				keyIndex := r.Intn(len(keys))
				key := []byte(keys[keyIndex])

				if r.Intn(100) < *getRatio {
					resp, err := clientDriver.GetBytes(key)
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
					} else {
						atomic.AddUint64(&stats.totalBytes, uint64(len(resp)))
					}
				} else {
					value := values[r.Intn(len(values))]

					resp, err := clientDriver.SetBytes(key, value, *ttl)
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
					} else {
						atomic.AddUint64(&stats.totalBytes, uint64(len(resp)))
					}
				}

				atomic.AddUint64(&stats.totalRequests, 1)
			}
		}(workerID)
	}

	wg.Wait()

	elapsed := time.Since(start)
	total := atomic.LoadUint64(&stats.totalRequests)
	errors := atomic.LoadUint64(&stats.totalErrors)
	bytesTransferred := atomic.LoadUint64(&stats.totalBytes)

	fmt.Println("========== LOAD TEST RESULT ==========")
	fmt.Println("Workers:", *workers)
	fmt.Println("Requests per worker:", *requestsPerWorker)
	fmt.Println("Total requests:", total)
	fmt.Println("Errors:", errors)
	fmt.Println("Duration:", elapsed)
	fmt.Printf("Requests/sec: %.2f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("MB/sec: %.2f\n", float64(bytesTransferred)/(1024*1024)/elapsed.Seconds())
	fmt.Println("======================================")
}

func makeValue(size int) []byte {
	return bytes.Repeat([]byte("x"), size)
}
