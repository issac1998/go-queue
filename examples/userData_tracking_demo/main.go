package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/async"
)

func main() {
	fmt.Println("🔍 UserData 追踪演示")
	fmt.Println("==================")

	// 1. 创建带异步IO的客户端
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

	// 2. 创建生产者
	producer := client.NewProducer(c)

	// 3. 发送消息并观察 userData 追踪
	fmt.Println("\n📤 发送消息并追踪 userData...")

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
			log.Printf("❌ 消息 %d 发送失败: %v (耗时: %v)", i, err, duration)
		} else {
			log.Printf("✅ 消息 %d 发送成功: Topic=%s, Partition=%d, Offset=%d (耗时: %v)",
				i, result.Topic, result.Partition, result.Offset, duration)
		}

		// 短暂延迟以观察异步行为
		time.Sleep(100 * time.Millisecond)
	}

	// 4. 显示统计信息
	fmt.Println("\n📊 异步IO统计信息:")
	stats := c.GetStats()
	fmt.Printf("  - Worker数量: %d\n", stats.AsyncIO.WorkerCount)
	fmt.Printf("  - 总连接数: %d\n", stats.AsyncIO.TotalConnections)
	fmt.Printf("  - 活跃连接数: %d\n", stats.AsyncIO.ActiveConnections)
	fmt.Printf("  - 提交队列大小: %d\n", stats.AsyncIO.SQSize)
	fmt.Printf("  - 完成队列大小: %d\n", stats.AsyncIO.CQSize)

	fmt.Println("\n🔍 UserData 追踪说明:")
	fmt.Println("  - 每个异步操作都有唯一的 userData (连接ID + 时间戳)")
	fmt.Println("  - userData 在回调函数中可用于请求关联和调试")
	fmt.Println("  - 可以通过 userData 实现请求去重和并发控制")
	fmt.Println("  - 在生产环境中可用于分布式追踪和性能监控")

	fmt.Println("\n✅ 基础 UserData 追踪演示完成!")

	// 5. 运行高级追踪演示
	DemoAdvancedTracking()
	
	fmt.Println("\n🎯 运行真实 UserData 使用演示...")
	DemoRealUserDataUsage()
	
	fmt.Println("\n🔥 运行回调 UserData 演示...")
	DemoCallbackUsage()
}