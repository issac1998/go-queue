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
	fmt.Println("🚀 Consumer Async I/O Demo")
	fmt.Println("=========================")

	// Create client with async I/O enabled
	config := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,

		// Enable async I/O for both producer and consumer
		EnableConnectionPool: true,
		EnableAsyncIO:        true,
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
			CQSize:         1024,
			BatchSize:      32,
			PollTimeout:    10 * time.Millisecond,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxConnections: 100,
		},
	}

	c := client.NewClient(config)
	defer c.Close()

	// Create admin to ensure topic exists
	admin := client.NewAdmin(c)
	topicName := "async-consumer-demo"

	fmt.Printf("\n📝 Creating topic '%s'...\n", topicName)
	createReq := client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 3,
		Replicas:   1,
	}

	if _, err := admin.CreateTopic(createReq); err != nil {
		log.Printf("Topic creation failed (might already exist): %v", err)
	}

	// Step 1: Produce some test messages using async I/O
	fmt.Println("\n📤 Producing test messages with async I/O...")
	producer := client.NewProducer(c)

	var wg sync.WaitGroup
	messageCount := 10

	for i := 0; i < messageCount; i++ {
		wg.Add(1)
		go func(msgID int) {
			defer wg.Done()

			msg := client.ProduceMessage{
				Topic:     topicName,
				Partition: int32(msgID % 3),
				Key:       []byte(fmt.Sprintf("key-%d", msgID)),
				Value:     []byte(fmt.Sprintf("async-message-%d created at %d", msgID, time.Now().UnixNano())),
			}

			start := time.Now()
			result, err := producer.Send(msg)
			duration := time.Since(start)

			if err != nil {
				log.Printf("❌ Message %d send failed: %v (took %v)", msgID, err, duration)
			} else {
				log.Printf("✅ Message %d sent: Partition=%d, Offset=%d (took %v)",
					msgID, result.Partition, result.Offset, duration)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("\n📤 All messages produced!")

	// Step 2: Consume messages using async I/O
	fmt.Println("\n📥 Consuming messages with async I/O...")
	consumer := client.NewConsumer(c)

	// Test async fetch from multiple partitions
	for partition := int32(0); partition < 3; partition++ {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()

			fmt.Printf("\n🔍 Fetching from partition %d with async I/O...\n", p)

			// Fetch messages from this partition
			start := time.Now()
			result, err := consumer.FetchFrom(topicName, p, 0)
			duration := time.Since(start)

			if err != nil {
				log.Printf("❌ Async fetch from partition %d failed: %v (took %v)", p, err, duration)
				return
			}

			log.Printf("✅ Async fetch from partition %d completed: %d messages (took %v)",
				p, len(result.Messages), duration)

			// Display messages
			for _, msg := range result.Messages {
				log.Printf("  📨 Partition %d, Offset %d: %s",
					msg.Partition, msg.Offset, string(msg.Value))
			}
		}(partition)
	}

	wg.Wait()

	// Step 3: Performance comparison
	fmt.Println("\n⚡ Performance Comparison...")

	// Test sync vs async performance
	syncTimes := make([]time.Duration, 5)
	asyncTimes := make([]time.Duration, 5)

	// Temporarily disable async I/O for sync test
	syncConfig := config
	syncConfig.EnableAsyncIO = false
	syncClient := client.NewClient(syncConfig)
	syncConsumer := client.NewConsumer(syncClient)

	// Sync fetch test
	for i := 0; i < 5; i++ {
		start := time.Now()
		_, err := syncConsumer.FetchFrom(topicName, 0, 0)
		syncTimes[i] = time.Since(start)
		if err != nil {
			log.Printf("Sync fetch %d failed: %v", i, err)
		}
	}

	// Async fetch test
	for i := 0; i < 5; i++ {
		start := time.Now()
		_, err := consumer.FetchFrom(topicName, 0, 0)
		asyncTimes[i] = time.Since(start)
		if err != nil {
			log.Printf("Async fetch %d failed: %v", i, err)
		}
	}

	syncClient.Close()

	// Calculate averages
	var syncTotal, asyncTotal time.Duration
	for i := 0; i < 5; i++ {
		syncTotal += syncTimes[i]
		asyncTotal += asyncTimes[i]
	}

	syncAvg := syncTotal / 5
	asyncAvg := asyncTotal / 5

	fmt.Printf("\n📊 Performance Results:\n")
	fmt.Printf("  🐌 Sync Average:  %v\n", syncAvg)
	fmt.Printf("  ⚡ Async Average: %v\n", asyncAvg)

	if syncAvg > asyncAvg {
		improvement := float64(syncAvg-asyncAvg) / float64(syncAvg) * 100
		fmt.Printf("  🚀 Async is %.1f%% faster!\n", improvement)
	} else {
		fmt.Printf("  📈 Results may vary based on network conditions\n")
	}

	// Step 4: Show async I/O statistics
	fmt.Println("\n📈 Async I/O Statistics:")
	stats := c.GetStats()

	if stats.AsyncIO.WorkerCount > 0 {
		fmt.Printf("  - Total Connections: %d\n", stats.AsyncIO.TotalConnections)
		fmt.Printf("  - Active Connections: %d\n", stats.AsyncIO.ActiveConnections)
		fmt.Printf("  - Worker Count: %d\n", stats.AsyncIO.WorkerCount)
		fmt.Printf("  - Submission Queue Size: %d\n", stats.AsyncIO.SQSize)
		fmt.Printf("  - Completion Queue Size: %d\n", stats.AsyncIO.CQSize)
	}

	fmt.Println("\n✅ Consumer Async I/O Demo completed successfully!")
	fmt.Println("\n🎯 Key Benefits Demonstrated:")
	fmt.Println("  ✅ Non-blocking consumer fetch operations")
	fmt.Println("  ✅ Concurrent fetching from multiple partitions")
	fmt.Println("  ✅ Improved performance through async I/O")
	fmt.Println("  ✅ Connection reuse and resource efficiency")
}