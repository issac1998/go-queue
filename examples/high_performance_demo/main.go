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
	fmt.Println("=== Go Queue 高性能客户端演示 ===")

	// 配置高性能客户端
	config := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,

		// 性能优化配置
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

		// 批量处理配置
		BatchSize:       100,
		BatchTimeout:    10 * time.Millisecond,
		MaxPendingBatch: 2000,
	}

	// 创建高性能客户端
	hpClient := client.NewClient(config)
	defer hpClient.Close()

	// 首先创建必要的topic
	fmt.Println("\n0. 创建测试topic...")
	createTestTopics(config)

	// 创建生产者
	producer := client.NewProducer(hpClient)

	fmt.Println("\n1. 性能基准测试...")
	runPerformanceBenchmark(producer)

	fmt.Println("\n2. 批量处理演示...")
	runBatchProcessingDemo(producer)

	fmt.Println("\n3. 获取性能统计...")
	showPerformanceStats(hpClient)

	fmt.Println("\n演示完成！")
}

// runPerformanceBenchmark 运行性能基准测试
func runPerformanceBenchmark(producer *client.Producer) {
	messageCount := 100 // 减少消息数量以便演示
	messageSize := 1024 // 1KB per message

	fmt.Printf("发送 %d 条消息，每条 %d 字节...\n", messageCount, messageSize)

	// 准备测试数据
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	start := time.Now()

	// 批量发送消息
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

	// 计算性能指标
	throughput := float64(messageCount) / duration.Seconds()
	dataRate := float64(messageCount*messageSize) / duration.Seconds() / (1024 * 1024) // MB/s

	fmt.Printf("✅ 发送完成!\n")
	fmt.Printf("   - 总时间: %v\n", duration)
	fmt.Printf("   - 吞吐量: %.2f msg/s\n", throughput)
	fmt.Printf("   - 数据速率: %.2f MB/s\n", dataRate)
}

// runBatchProcessingDemo 运行批量处理演示
func runBatchProcessingDemo(producer *client.Producer) {
	fmt.Println("演示批量处理优化...")

	// 创建批量消息
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

	fmt.Printf("✅ 批量发送完成!\n")
	fmt.Printf("   - 消息数量: %d\n", len(messages))
	fmt.Printf("   - 发送时间: %v\n", duration)
	fmt.Printf("   - 起始偏移量: %d\n", result.Offset)
}

// showPerformanceStats 显示性能统计
func showPerformanceStats(client *client.Client) {
	fmt.Println("=== 性能统计信息 ===")

	stats := client.GetStats()

	// 客户端统计
	fmt.Printf("📊 客户端统计:\n")
	fmt.Printf("  - Topic缓存数: %d\n", stats.TopicCount)
	fmt.Printf("  - 元数据TTL: %v\n", stats.MetadataTTL)

	// 连接池统计
	if stats.ConnectionPool.TotalConnections > 0 {
		fmt.Printf("\n🔗 连接池统计:\n")
		fmt.Printf("  - 总连接数: %d\n", stats.ConnectionPool.TotalConnections)
		fmt.Printf("  - 活跃连接数: %d\n", stats.ConnectionPool.ActiveConnections)
		fmt.Printf("  - Broker连接池数: %d\n", len(stats.ConnectionPool.BrokerStats))

		for addr, brokerStats := range stats.ConnectionPool.BrokerStats {
			fmt.Printf("    - %s: 总连接=%d, 活跃=%d, 池大小=%d\n",
				addr, brokerStats.TotalConnections, brokerStats.ActiveConnections, brokerStats.PoolSize)
		}
	}

	// 异步IO统计
	if stats.AsyncIO.WorkerCount > 0 {
		fmt.Printf("\n⚡ 异步IO统计:\n")
		fmt.Printf("  - 总连接数: %d\n", stats.AsyncIO.TotalConnections)
		fmt.Printf("  - 活跃连接数: %d\n", stats.AsyncIO.ActiveConnections)
		fmt.Printf("  - Worker数量: %d\n", stats.AsyncIO.WorkerCount)
		fmt.Printf("  - 提交队列大小: %d\n", stats.AsyncIO.SQSize)
		fmt.Printf("  - 完成队列大小: %d\n", stats.AsyncIO.CQSize)
	}

	// 计算效率指标
	fmt.Printf("\n🚀 性能优化特性:\n")
	fmt.Println("✅ 连接池: 复用连接，减少建立/关闭开销")
	fmt.Println("✅ 异步IO: 非阻塞操作，提高并发性能")
	fmt.Println("✅ 批量处理: 减少网络往返，提高吞吐量")
	fmt.Println("✅ 智能缓冲: 自动调节批量大小和时间")
}

// createTestTopics 创建测试需要的topic
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

	// 等待topic创建完成
	time.Sleep(2 * time.Second)
}
