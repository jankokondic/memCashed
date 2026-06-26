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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	totalRequests uint64
	totalErrors   uint64
	totalBytes    uint64
}

func main() {
	addr := flag.String("addr", "127.0.0.1:11211", "memcached address")
	workers := flag.Int("workers", 64, "number of concurrent workers")
	duration := flag.Duration("duration", 60*time.Second, "benchmark duration")
	warmup := flag.Duration("warmup", 5*time.Second, "warmup duration")
	keyCount := flag.Int("keys", 2000, "number of keys")
	valueSize := flag.Int("size", 1024, "value size in bytes")
	getRatio := flag.Int("getratio", 70, "GET percentage")
	pipeline := flag.Int("pipeline", 16, "number of GET responses to pipeline before reading")
	ttl := flag.Int("ttl", 0, "TTL seconds, 0 means no expiration")
	flag.Parse()

	value := bytes.Repeat([]byte("x"), *valueSize)

	keys := make([]string, *keyCount)
	for i := 0; i < *keyCount; i++ {
		keys[i] = fmt.Sprintf("key:%d", i)
	}

	fmt.Println("Preloading memcached...")

	err := preload(*addr, keys, value, *ttl)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Preload finished.")
	fmt.Println("Warmup started...")

	runAsync(*addr, keys, value, *workers, *warmup, *getRatio, *pipeline, *ttl)

	fmt.Println("Warmup finished.")
	fmt.Println("Benchmark started...")

	start := time.Now()
	stats := runAsync(*addr, keys, value, *workers, *duration, *getRatio, *pipeline, *ttl)
	elapsed := time.Since(start)

	total := atomic.LoadUint64(&stats.totalRequests)
	errors := atomic.LoadUint64(&stats.totalErrors)
	bytesTransferred := atomic.LoadUint64(&stats.totalBytes)

	fmt.Println("========== MEMCACHED ASYNC RESULT ==========")
	fmt.Println("Address:", *addr)
	fmt.Println("Workers:", *workers)
	fmt.Println("Duration:", elapsed)
	fmt.Println("GET ratio:", *getRatio)
	fmt.Println("Pipeline:", *pipeline)
	fmt.Println("Keys:", *keyCount)
	fmt.Println("Value size:", *valueSize, "bytes")
	fmt.Println("Total requests:", total)
	fmt.Println("Errors:", errors)
	fmt.Printf("Requests/sec: %.2f\n", float64(total)/elapsed.Seconds())
	fmt.Printf("MB/sec: %.2f\n", float64(bytesTransferred)/(1024*1024)/elapsed.Seconds())
	fmt.Println("===========================================")
}

func preload(addr string, keys []string, value []byte, ttl int) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = conn.(*net.TCPConn).SetNoDelay(true)

	reader := bufio.NewReaderSize(conn, 1024*1024)
	writer := bufio.NewWriterSize(conn, 1024*1024)

	for _, key := range keys {
		_, err := fmt.Fprintf(writer, "set %s 0 %d %d\r\n", key, ttl, len(value))
		if err != nil {
			return err
		}

		_, err = writer.Write(value)
		if err != nil {
			return err
		}

		_, err = writer.WriteString("\r\n")
		if err != nil {
			return err
		}

		err = writer.Flush()
		if err != nil {
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

func runAsync(
	addr string,
	keys []string,
	value []byte,
	workers int,
	duration time.Duration,
	getRatio int,
	pipeline int,
	ttl int,
) Stats {
	var stats Stats
	var wg sync.WaitGroup

	stop := time.Now().Add(duration)

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				atomic.AddUint64(&stats.totalErrors, 1)
				return
			}
			defer conn.Close()

			if tcpConn, ok := conn.(*net.TCPConn); ok {
				_ = tcpConn.SetNoDelay(true)
				_ = tcpConn.SetReadBuffer(1024 * 1024)
				_ = tcpConn.SetWriteBuffer(1024 * 1024)
			}

			reader := bufio.NewReaderSize(conn, 1024*1024)
			writer := bufio.NewWriterSize(conn, 1024*1024)

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			pendingGets := 0

			for time.Now().Before(stop) {
				key := keys[r.Intn(len(keys))]

				if r.Intn(100) < getRatio {
					_, err := writer.WriteString("get ")
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
						return
					}

					_, err = writer.WriteString(key)
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
						return
					}

					_, err = writer.WriteString("\r\n")
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
						return
					}

					pendingGets++
					atomic.AddUint64(&stats.totalRequests, 1)

					if pendingGets >= pipeline {
						if err := writer.Flush(); err != nil {
							atomic.AddUint64(&stats.totalErrors, 1)
							return
						}

						readGETResponses(reader, pendingGets, &stats)
						pendingGets = 0
					}
				} else {
					_, err := fmt.Fprintf(writer, "set %s 0 %d %d noreply\r\n", key, ttl, len(value))
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
						return
					}

					_, err = writer.Write(value)
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
						return
					}

					_, err = writer.WriteString("\r\n")
					if err != nil {
						atomic.AddUint64(&stats.totalErrors, 1)
						return
					}

					atomic.AddUint64(&stats.totalBytes, uint64(len(value)))
					atomic.AddUint64(&stats.totalRequests, 1)
				}
			}

			if pendingGets > 0 {
				if err := writer.Flush(); err != nil {
					atomic.AddUint64(&stats.totalErrors, 1)
					return
				}

				readGETResponses(reader, pendingGets, &stats)
			}

			_ = writer.Flush()
		}(workerID)
	}

	wg.Wait()

	return stats
}

func readGETResponses(reader *bufio.Reader, count int, stats *Stats) {
	for i := 0; i < count; i++ {
		n, err := readOneGETResponse(reader)
		if err != nil {
			atomic.AddUint64(&stats.totalErrors, 1)
			continue
		}

		atomic.AddUint64(&stats.totalBytes, uint64(n))
	}
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

	_, err = io.ReadFull(reader, buf)
	if err != nil {
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

func makeValue(size int) []byte {
	return bytes.Repeat([]byte("x"), size)
}
