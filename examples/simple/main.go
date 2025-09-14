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
	fmt.Println("=== Go Queue 简单客户端演示 ===")

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
	fmt.Println("\n1. 创建Topic...")
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
		fmt.Printf("✅ Topic创建成功: %s\n", createResult.Name)
	}

	// Create Producer
	fmt.Println("\n2. 异步IO长连接演示...")
	producer := client.NewProducer(c)

	// High concurrency send test - demonstrate async IO persistent connection advantages
	fmt.Println("  发送100条消息测试异步IO性能...")

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
					fmt.Printf("  ✅ 消息 %d: Partition=%d, Offset=%d\n", msgId, result.Partition, result.Offset)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("\n📊 异步IO性能测试结果:\n")
	fmt.Printf("  - 总消息数: 100\n")
	fmt.Printf("  - 成功数: %d\n", successCount)
	fmt.Printf("  - 失败数: %d\n", errorCount)
	fmt.Printf("  - 总用时: %v\n", duration)
	fmt.Printf("  - 平均延迟: %v/msg\n", duration/100)
	fmt.Printf("  - 吞吐量: %.2f msg/s\n", float64(successCount)/duration.Seconds())

	// Batch send demonstration
	fmt.Println("\n3. 批量发送演示...")
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
		fmt.Printf("✅ 批量发送成功: 10条消息，用时 %v, 起始Offset=%d\n",
			batchDuration, batchResult.Offset)
	}

	// Create Consumer
	fmt.Println("\n4. 消费消息...")
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

		fmt.Printf("✅ 分区 %d: 消费到 %d 条消息\n", partition, len(fetchResult.Messages))
		if len(fetchResult.Messages) > 0 {
			// Show first 3 messages
			for i, message := range fetchResult.Messages[:min(3, len(fetchResult.Messages))] {
				fmt.Printf("   消息 %d: Offset=%d, Value=%s\n", i, message.Offset, string(message.Value))
			}
			if len(fetchResult.Messages) > 3 {
				fmt.Printf("   ... 还有 %d 条消息\n", len(fetchResult.Messages)-3)
			}
		}
	}

	// Show client statistics
	fmt.Println("\n5. 性能统计...")
	stats := c.GetStats()
	fmt.Printf("📊 客户端统计:\n")
	fmt.Printf("  - Topic缓存数: %d\n", stats.TopicCount)
	fmt.Printf("  - 元数据TTL: %v\n", stats.MetadataTTL)

	if stats.ConnectionPool.TotalConnections > 0 {
		fmt.Printf("\n🔗 连接池统计:\n")
		fmt.Printf("  - 总连接数: %d\n", stats.ConnectionPool.TotalConnections)
		fmt.Printf("  - 活跃连接数: %d\n", stats.ConnectionPool.ActiveConnections)
		fmt.Printf("  - Broker连接池数: %d\n", len(stats.ConnectionPool.BrokerStats))
	}

	if stats.AsyncIO.WorkerCount > 0 {
		fmt.Printf("\n⚡ 异步IO统计:\n")
		fmt.Printf("  - Worker数量: %d\n", stats.AsyncIO.WorkerCount)
		fmt.Printf("  - 异步连接总数: %d\n", stats.AsyncIO.TotalConnections)
		fmt.Printf("  - 异步连接活跃数: %d\n", stats.AsyncIO.ActiveConnections)
		fmt.Printf("  - 提交队列大小: %d\n", stats.AsyncIO.SQSize)
		fmt.Printf("  - 完成队列大小: %d\n", stats.AsyncIO.CQSize)
	}

	fmt.Println("\n✅ 演示完成！")
	fmt.Println("\n🚀 新异步IO架构特性:")
	fmt.Println("  ✅ 长连接复用: 每个broker维护一个长连接，避免频繁建立/关闭")
	fmt.Println("  ✅ 事件驱动: 基于事件循环的异步处理，高并发性能")
	fmt.Println("  ✅ 智能降级: 异步连接失败时自动降级到连接池")
	fmt.Println("  ✅ 资源管理: 连接生命周期管理，优雅关闭")
	fmt.Println("  ✅ 双重保障: 异步IO + 连接池，确保高可用性")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
