package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"sync/atomic"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/async"
	"github.com/issac1998/go-queue/internal/pool"
)

func main() {
	fmt.Println("=== Go Queue Simple Client Demo ===")

	// Create client config (connection pool and async IO enabled by default)
	config := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,

		// Performance optimization config (optional)
		EnableConnectionPool: true,
		EnableAsyncIO:        true, // Enable async IO persistent connections
		ConnectionPool: pool.ConnectionPoolConfig{
			MaxConnections:    10,
			MinConnections:    2,
			ConnectionTimeout: 3 * time.Second,
			IdleTimeout:       5 * time.Minute,
			MaxLifetime:       15 * time.Minute,
		},
		AsyncIO: async.AsyncIOConfig{
			WorkerCount:    4,
			SQSize:         512,
			CQSize:         512,
			BatchSize:      100,
			PollTimeout:    10 * time.Millisecond,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxConnections: 100,
		},
		BatchSize:       50,
		BatchTimeout:    5 * time.Millisecond,
		MaxPendingBatch: 500,
	}

	// Create client
	c := client.NewClient(config)
	defer c.Close()

	// Create admin client
	admin := client.NewAdmin(c)

	// Create topic
	fmt.Println("\n1. Creating Topic...")
	createReq := client.CreateTopicRequest{
		Name:       "async-demo",
		Partitions: 3,
		Replicas:   1,
	}

	createResult, err := admin.CreateTopic(createReq)
	if err != nil {
		log.Printf("Failed to create topic (might already exist): %v", err)
	} else if createResult.Error != nil {
		log.Printf("Failed to create topic: %v", createResult.Error)
	} else {
		fmt.Printf("✅ Topic created successfully: %s\n", createResult.Name)
	}

	// Create Producer
	fmt.Println("\n2. Async IO persistent connection demo...")
	producer := client.NewProducer(c)

	// High concurrency send test - demonstrate async IO persistent connection advantages
	fmt.Println("  Sending 100 messages to test async IO performance...")

	start := time.Now()
	var wg sync.WaitGroup
	successCount := int64(0)
	errorCount := int64(0)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(msgId int) {
			defer wg.Done()

			msg := client.ProduceMessage{
				Topic:     "async-demo",
				Partition: int32(msgId % 3), // Distribute to 3 partitions
				Value:     []byte(fmt.Sprintf("Async message %d - %s", msgId, time.Now().Format("15:04:05.000"))),
			}

			result, err := producer.Send(msg)
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				log.Printf("Message %d failed: %v", msgId, err)
			} else {
				atomic.AddInt64(&successCount, 1)
				if msgId%20 == 0 { // Print every 20 messages
					fmt.Printf("  ✅ Message %d: Partition=%d, Offset=%d\n", msgId, result.Partition, result.Offset)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n📊 Async IO performance test results:\n")
	fmt.Printf("  - Total messages: 100\n")
	fmt.Printf("  - Success count: %d\n", successCount)
	fmt.Printf("  - Error count: %d\n", errorCount)
	fmt.Printf("  - Total time: %v\n", duration)
	fmt.Printf("  - Average latency: %v/msg\n", duration/100)
	fmt.Printf("  - Throughput: %.2f msg/s\n", float64(successCount)/duration.Seconds())

	// Batch send demonstration
	fmt.Println("\n3. Batch send demo...")
	messages := make([]client.ProduceMessage, 10)
	for i := range messages {
		messages[i] = client.ProduceMessage{
			Topic:     "async-demo",
			Partition: 1,
			Value:     []byte(fmt.Sprintf("Batch message %d", i)),
		}
	}

	batchStart := time.Now()
	batchResult, err := producer.SendBatch(messages)
	batchDuration := time.Since(batchStart)

	if err != nil {
		log.Printf("Failed to send batch: %v", err)
	} else {
		fmt.Printf("✅ Batch send successful: 10 messages, time %v, starting Offset=%d\n",
			batchDuration, batchResult.Offset)
	}

	// Create Consumer
	fmt.Println("\n4. Consuming messages...")
	consumer := client.NewConsumer(c)

	// Consume messages from different partitions
	for partition := int32(0); partition < 3; partition++ {
		fetchReq := client.FetchRequest{
			Topic:     "async-demo",
			Partition: partition,
			Offset:    0,
			MaxBytes:  4096,
		}

		fetchResult, err := consumer.Fetch(fetchReq)
		if err != nil {
			log.Printf("Failed to fetch from partition %d: %v", partition, err)
			continue
		}

		fmt.Printf("✅ Partition %d: Consumed %d messages\n", partition, len(fetchResult.Messages))
		if len(fetchResult.Messages) > 0 {
			// Show first 3 messages
			for i, message := range fetchResult.Messages[:min(3, len(fetchResult.Messages))] {
				fmt.Printf("   Message %d: Offset=%d, Value=%s\n", i, message.Offset, string(message.Value))
			}
			if len(fetchResult.Messages) > 3 {
				fmt.Printf("   ... %d more messages\n", len(fetchResult.Messages)-3)
			}
		}
	}

	// Show client statistics
	fmt.Println("\n5. Performance statistics...")
	stats := c.GetStats()
	fmt.Printf("📊 Client statistics:\n")
	fmt.Printf("  - Topic cache count: %d\n", stats.TopicCount)
	fmt.Printf("  - Metadata TTL: %v\n", stats.MetadataTTL)

	if stats.ConnectionPool.TotalConnections > 0 {
		fmt.Printf("\n🔗 Connection pool statistics:\n")
		fmt.Printf("  - Total connections: %d\n", stats.ConnectionPool.TotalConnections)
		fmt.Printf("  - Active connections: %d\n", stats.ConnectionPool.ActiveConnections)
		fmt.Printf("  - Broker connection pools: %d\n", len(stats.ConnectionPool.BrokerStats))
	}

	if stats.AsyncIO.WorkerCount > 0 {
		fmt.Printf("\n⚡ Async IO statistics:\n")
		fmt.Printf("  - Worker count: %d\n", stats.AsyncIO.WorkerCount)
		fmt.Printf("  - Total async connections: %d\n", stats.AsyncIO.TotalConnections)
		fmt.Printf("  - Active async connections: %d\n", stats.AsyncIO.ActiveConnections)
		fmt.Printf("  - Submit queue size: %d\n", stats.AsyncIO.SQSize)
		fmt.Printf("  - Completion queue size: %d\n", stats.AsyncIO.CQSize)
	}

	fmt.Println("\n✅ Demo completed!")
	fmt.Println("\n🚀 New async IO architecture features:")
	fmt.Println("  ✅ Connection reuse: Maintains one persistent connection per broker, avoiding frequent connect/disconnect")
	fmt.Println("  ✅ Event-driven: Event loop based async processing for high concurrency performance")
	fmt.Println("  ✅ Smart fallback: Automatically falls back to connection pool when async connection fails")
	fmt.Println("  ✅ Resource management: Connection lifecycle management with graceful shutdown")
	fmt.Println("  ✅ Dual guarantee: Async IO + connection pool ensures high availability")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
