package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/async"
	"github.com/issac1998/go-queue/internal/pool"
)

func main() {
	fmt.Println("=== Go Queue High Performance Client Demo ===")

	// Configure high performance client
	config := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,

		// Performance optimization configuration
		EnableConnectionPool: true,
		EnableAsyncIO:        true,
		ConnectionPool: pool.ConnectionPoolConfig{
			MaxConnections:    20,
			MinConnections:    5,
			ConnectionTimeout: 3 * time.Second,
			IdleTimeout:       10 * time.Minute,
			MaxLifetime:       30 * time.Minute,
		},
		AsyncIO: async.AsyncIOConfig{
			WorkerCount:    8,
			SQSize:         1024,
			CQSize:         1024,
			BatchSize:      200,
			PollTimeout:    5 * time.Millisecond,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			MaxConnections: 1000,
		},

		// Batch processing configuration
		BatchSize:       100,
		BatchTimeout:    10 * time.Millisecond,
		MaxPendingBatch: 2000,
	}

	// Create high performance client
	hpClient := client.NewClient(config)
	defer hpClient.Close()

	// First create necessary topics
	fmt.Println("\n0. Creating test topics...")
	createTestTopics(config)

	// Create producer
	producer := client.NewProducer(hpClient)

	fmt.Println("\n1. Performance benchmark test...")
	runPerformanceBenchmark(producer)

	fmt.Println("\n2. Batch processing demo...")
	runBatchProcessingDemo(producer)

	fmt.Println("\n3. Get performance statistics...")
	showPerformanceStats(hpClient)

	fmt.Println("\nDemo completed!")
}

// runPerformanceBenchmark runs performance benchmark test
func runPerformanceBenchmark(producer *client.Producer) {
	messageCount := 100 // Reduce message count for demo
	messageSize := 1024 // 1KB per message

	fmt.Printf("Sending %d messages, %d bytes each...\n", messageCount, messageSize)

	// Prepare test data
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	start := time.Now()

	// Batch send messages
	var wg sync.WaitGroup
	batchSize := 10
	for i := 0; i < messageCount; i += batchSize {
		wg.Add(1)
		go func(startIdx int) {
			defer wg.Done()

			endIdx := startIdx + batchSize
			if endIdx > messageCount {
				endIdx = messageCount
			}

			messages := make([]client.ProduceMessage, endIdx-startIdx)
			for j := startIdx; j < endIdx; j++ {
				messages[j-startIdx] = client.ProduceMessage{
					Topic:     "performance-test",
					Partition: 0,
					Value:     payload,
				}
			}

			_, err := producer.SendBatch(messages)
			if err != nil {
				log.Printf("Failed to send batch: %v", err)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	// Calculate performance metrics
	throughput := float64(messageCount) / duration.Seconds()
	dataRate := float64(messageCount*messageSize) / duration.Seconds() / (1024 * 1024) // MB/s

	fmt.Printf("✅ Send completed!\n")
	fmt.Printf("   - Total time: %v\n", duration)
	fmt.Printf("   - Throughput: %.2f msg/s\n", throughput)
	fmt.Printf("   - Data rate: %.2f MB/s\n", dataRate)
}

// runBatchProcessingDemo runs batch processing demo
func runBatchProcessingDemo(producer *client.Producer) {
	fmt.Println("Demonstrating batch processing optimization...")

	// Create batch messages
	messages := make([]client.ProduceMessage, 50)
	for i := range messages {
		messages[i] = client.ProduceMessage{
			Topic:     "batch-test",
			Partition: 0,
			Value:     []byte(fmt.Sprintf("Batch message %d - %s", i, time.Now().Format("15:04:05.000"))),
		}
	}

	start := time.Now()
	result, err := producer.SendBatch(messages)
	duration := time.Since(start)

	if err != nil {
		log.Printf("Batch send failed: %v", err)
		return
	}

	fmt.Printf("✅ Batch send completed!\n")
	fmt.Printf("   - Message count: %d\n", len(messages))
	fmt.Printf("   - Send time: %v\n", duration)
	fmt.Printf("   - Starting offset: %d\n", result.Offset)
}

// showPerformanceStats displays performance statistics
func showPerformanceStats(client *client.Client) {
	fmt.Println("=== Performance Statistics ===")

	stats := client.GetStats()

	// Client statistics
	fmt.Printf("📊 Client Statistics:\n")
	fmt.Printf("  - Topic cache count: %d\n", stats.TopicCount)
	fmt.Printf("  - Metadata TTL: %v\n", stats.MetadataTTL)

	// Connection pool statistics
	if stats.ConnectionPool.TotalConnections > 0 {
		fmt.Printf("\n🔗 Connection Pool Statistics:\n")
		fmt.Printf("  - Total connections: %d\n", stats.ConnectionPool.TotalConnections)
		fmt.Printf("  - Active connections: %d\n", stats.ConnectionPool.ActiveConnections)
		fmt.Printf("  - Broker connection pools: %d\n", len(stats.ConnectionPool.BrokerStats))

		for addr, brokerStats := range stats.ConnectionPool.BrokerStats {
			fmt.Printf("    - %s: Total=%d, Active=%d, Pool size=%d\n",
				addr, brokerStats.TotalConnections, brokerStats.ActiveConnections, brokerStats.PoolSize)
		}
	}

	// Async IO statistics
	if stats.AsyncIO.WorkerCount > 0 {
		fmt.Printf("\n⚡ Async IO Statistics:\n")
		fmt.Printf("  - Total connections: %d\n", stats.AsyncIO.TotalConnections)
		fmt.Printf("  - Active connections: %d\n", stats.AsyncIO.ActiveConnections)
		fmt.Printf("  - Worker count: %d\n", stats.AsyncIO.WorkerCount)
		fmt.Printf("  - Submit queue size: %d\n", stats.AsyncIO.SQSize)
		fmt.Printf("  - Completion queue size: %d\n", stats.AsyncIO.CQSize)
	}

	// Calculate efficiency metrics
	fmt.Printf("\n🚀 Performance Optimization Features:\n")
	fmt.Println("✅ Connection Pool: Reuse connections, reduce establishment/closure overhead")
	fmt.Println("✅ Async IO: Non-blocking operations, improve concurrent performance")
	fmt.Println("✅ Batch Processing: Reduce network round trips, improve throughput")
	fmt.Println("✅ Smart Buffering: Automatically adjust batch size and timing")
}

// createTestTopics creates topics needed for testing
func createTestTopics(config client.ClientConfig) {
	baseClient := client.NewClient(config)
	admin := client.NewAdmin(baseClient)

	topics := []string{
		"performance-test",
		"small-messages",
		"medium-messages",
		"large-messages",
		"async-demo",
	}

	for _, topic := range topics {
		_, err := admin.CreateTopic(client.CreateTopicRequest{
			Name:       topic,
			Partitions: 3,
			Replicas:   1,
		})
		if err != nil {
			fmt.Printf("Failed to create topic %s: %v (may already exist)\n", topic, err)
		} else {
			fmt.Printf("✓ Created topic: %s\n", topic)
		}
	}

	// Wait for topic creation to complete
	time.Sleep(2 * time.Second)
}
