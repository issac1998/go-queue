package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/async"
)

func main() {
	fmt.Println("🔍 UserData Tracking Demo")
	fmt.Println("==================")

	// 1. Create client with async IO
	clientConfig := client.ClientConfig{
		BrokerAddrs:          []string{"localhost:9092"},
		Timeout:              5 * time.Second,
		EnableConnectionPool: true,
		EnableAsyncIO:        true,
		AsyncIO: async.AsyncIOConfig{
			WorkerCount:    2,
			SQSize:         512,
			CQSize:         1024,
			BatchSize:      16,
			PollTimeout:    50 * time.Millisecond,
			MaxConnections: 100,
		},
	}

	c := client.NewClient(clientConfig)
	defer c.Close()

	// 2. Create producer
	producer := client.NewProducer(c)

	// 3. Send messages and observe userData tracking
	fmt.Println("\n📤 Sending messages and tracking userData...")

	for i := 0; i < 3; i++ {
		msg := client.ProduceMessage{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("message-%d with timestamp %d", i, time.Now().UnixNano())),
		}

		start := time.Now()
		result, err := producer.Send(msg)
		duration := time.Since(start)

		if err != nil {
			log.Printf("❌ Message %d send failed: %v (duration: %v)", i, err, duration)
		} else {
			log.Printf("✅ Message %d sent successfully: Topic=%s, Partition=%d, Offset=%d (duration: %v)",
				i, result.Topic, result.Partition, result.Offset, duration)
		}

		// Brief delay to observe async behavior
		time.Sleep(100 * time.Millisecond)
	}

	// 4. Show statistics
	fmt.Println("\n📊 Async IO statistics:")
	stats := c.GetStats()
	fmt.Printf("  - Worker count: %d\n", stats.AsyncIO.WorkerCount)
	fmt.Printf("  - Total connections: %d\n", stats.AsyncIO.TotalConnections)
	fmt.Printf("  - Active connections: %d\n", stats.AsyncIO.ActiveConnections)
	fmt.Printf("  - Submit queue size: %d\n", stats.AsyncIO.SQSize)
	fmt.Printf("  - Completion queue size: %d\n", stats.AsyncIO.CQSize)

	fmt.Println("\n🔍 UserData tracking explanation:")
	fmt.Println("  - Each async operation has a unique userData (connection ID + timestamp)")
	fmt.Println("  - userData is available in callback functions for request correlation and debugging")
	fmt.Println("  - Can implement request deduplication and concurrency control through userData")
	fmt.Println("  - Can be used for distributed tracing and performance monitoring in production")

	fmt.Println("\n✅ Basic UserData tracking demo completed!")

	// 5. Run advanced tracking demo
	DemoAdvancedTracking()
	
	fmt.Println("\n🎯 Running real UserData usage demo...")
	DemoRealUserDataUsage()
	
	fmt.Println("\n🔥 Running callback UserData demo...")
	DemoCallbackUsage()
}