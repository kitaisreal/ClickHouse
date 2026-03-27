package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func execQuery(client *http.Client, addr, query string) error {
	resp, err := client.Post(addr, "text/plain", strings.NewReader(query))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		buf := make([]byte, 1024)
		n, _ := resp.Body.Read(buf)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(buf[:n]))
	}
	return nil
}

func worker(client *http.Client, addr string, tasks <-chan int, created *atomic.Int64, errors *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := range tasks {
		q1 := fmt.Sprintf("CREATE TABLE IF NOT EXISTS test_table_%d (id UInt64, value String) ENGINE=MergeTree ORDER BY id SETTINGS storage_policy = 's3_main'", i)
		q2 := fmt.Sprintf("INSERT INTO test_table_%d VALUES (0, 'Value_%d')", i, i)
		q3 := fmt.Sprintf("SELECT * FROM test_table_%d FORMAT Null", i)

		for _, q := range []string{q1, q2, q3} {
			if err := execQuery(client, addr, q); err != nil {
				fmt.Fprintf(os.Stderr, "table %d error: %v\n", i, err)
				errors.Add(1)
				break
			}
		}
		c := created.Add(1)
		if c%1000 == 0 {
			fmt.Fprintf(os.Stderr, "progress: %d tables\n", c)
		}
	}
}

func main() {
	addr := "http://localhost:8123/"
	numTables := 100000
	concurrency := 64

	if v := os.Getenv("CLICKHOUSE_URL"); v != "" {
		addr = v
	}
	if v := os.Getenv("NUM_TABLES"); v != "" {
		numTables, _ = strconv.Atoi(v)
	}
	if v := os.Getenv("CONCURRENCY"); v != "" {
		concurrency, _ = strconv.Atoi(v)
	}

	fmt.Fprintf(os.Stderr, "Populating %d tables with %d workers against %s\n", numTables, concurrency, addr)

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	tasks := make(chan int, concurrency*2)
	var created, errors atomic.Int64
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker(client, addr, tasks, &created, &errors, &wg)
	}

	for i := 0; i < numTables; i++ {
		tasks <- i
	}
	close(tasks)
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "Done: %d tables created, %d errors, elapsed %s\n", created.Load(), errors.Load(), elapsed)
}
