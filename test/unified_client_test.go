package test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
)

// TestUnifiedClientSingleMode 测试单机模式
func TestUnifiedClientSingleMode(t *testing.T) {
	log.Println("=== 测试统一客户端 - 单机模式 ===")

	// 创建单机模式客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	// 等待初始化
	time.Sleep(1 * time.Second)

	// 获取统计信息
	stats := c.GetStats()
	log.Printf("客户端模式: 集群模式=%v", stats.IsClusterMode)

	// 测试Producer
	testProducer(t, c, "单机模式")

	// 测试Consumer
	testConsumer(t, c, "单机模式")

	log.Println("✅ 单机模式测试完成")
}

// TestUnifiedClientClusterMode 测试集群模式
func TestUnifiedClientClusterMode(t *testing.T) {
	log.Println("=== 测试统一客户端 - 集群模式 ===")

	// 创建集群模式客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Timeout:     10 * time.Second,
	})

	// 等待集群发现
	time.Sleep(2 * time.Second)

	// 获取统计信息
	stats := c.GetStats()
	log.Printf("客户端模式: 集群模式=%v, Leader=%s", stats.IsClusterMode, stats.CurrentLeader)

	// 测试Producer（应该路由到Leader）
	testProducer(t, c, "集群模式")

	// 测试Consumer（应该负载均衡到Follower）
	testConsumer(t, c, "集群模式")

	// 查看读写统计
	finalStats := c.GetStats()
	log.Printf("📊 最终统计: 写请求=%d, 读请求=%d, Leader切换=%d",
		finalStats.WriteRequests, finalStats.ReadRequests, finalStats.LeaderSwitches)

	log.Println("✅ 集群模式测试完成")
}

// TestClientCompatibility 测试向后兼容性
func TestClientCompatibility(t *testing.T) {
	log.Println("=== 测试向后兼容性 ===")

	// 使用原有的单机配置方式
	oldStyleClient := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	// 使用新的集群配置方式
	newStyleClient := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"}, // 单个地址的集群配置
		Timeout:     5 * time.Second,
	})

	// 两种方式应该都能正常工作
	testBasicOperation(t, oldStyleClient, "旧式配置")
	testBasicOperation(t, newStyleClient, "新式配置")

	log.Println("✅ 向后兼容性测试完成")
}

// TestFailoverBehavior 测试故障切换行为
func TestFailoverBehavior(t *testing.T) {
	log.Println("=== 测试故障切换行为 ===")

	// 创建包含不存在节点的集群客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9999", "localhost:8888"}, // 后两个不存在
		Timeout:     3 * time.Second,
	})

	time.Sleep(2 * time.Second)

	// 应该能够正常工作（自动跳过不健康的节点）
	testBasicOperation(t, c, "故障切换")

	stats := c.GetStats()
	log.Printf("故障切换统计: Leader切换=%d", stats.LeaderSwitches)

	log.Println("✅ 故障切换测试完成")
}

// 辅助测试函数

func testProducer(t *testing.T, c *client.Client, mode string) {
	log.Printf("--- 测试Producer (%s) ---", mode)

	producer := client.NewProducer(c)

	for i := 0; i < 3; i++ {
		message := fmt.Sprintf("test-message-%d", i)
		result, err := producer.Send(client.ProduceMessage{
			Topic:     "test-topic",
			Partition: 0,
			Value:     []byte(message),
		})

		if err != nil {
			log.Printf("⚠️  Producer发送失败 (%s): %v", mode, err)
		} else {
			log.Printf("✅ Producer发送成功 (%s): offset=%d", mode, result.Offset)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func testConsumer(t *testing.T, c *client.Client, mode string) {
	log.Printf("--- 测试Consumer (%s) ---", mode)

	consumer := client.NewConsumer(c)

	for i := 0; i < 3; i++ {
		result, err := consumer.Fetch(client.FetchRequest{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			MaxBytes:  1024,
		})

		if err != nil {
			log.Printf("⚠️  Consumer获取失败 (%s): %v", mode, err)
		} else {
			log.Printf("✅ Consumer获取成功 (%s): 消息数=%d", mode, len(result.Messages))
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func testBasicOperation(t *testing.T, c *client.Client, configType string) {
	log.Printf("--- 基础操作测试 (%s) ---", configType)

	// 创建管理员客户端
	admin := client.NewAdmin(c)

	// 尝试创建topic
	_, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "compatibility-test",
		Partitions: 1,
		Replicas:   1,
	})

	if err != nil {
		log.Printf("⚠️  创建Topic失败 (%s): %v", configType, err)
	} else {
		log.Printf("✅ 创建Topic成功 (%s)", configType)
	}

	// 简单的produce/consume测试
	producer := client.NewProducer(c)
	result, err := producer.Send(client.ProduceMessage{
		Topic:     "compatibility-test",
		Partition: 0,
		Value:     []byte("compatibility-test-message"),
	})

	if err != nil {
		log.Printf("⚠️  兼容性发送失败 (%s): %v", configType, err)
	} else {
		log.Printf("✅ 兼容性发送成功 (%s): offset=%d", configType, result.Offset)
	}
}

// BenchmarkUnifiedClient 性能基准测试
func BenchmarkUnifiedClient(b *testing.B) {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Timeout:     5 * time.Second,
	})

	time.Sleep(1 * time.Second) // 等待初始化

	producer := client.NewProducer(c)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, err := producer.Send(client.ProduceMessage{
				Topic:     "benchmark-topic",
				Partition: 0,
				Value:     []byte(fmt.Sprintf("benchmark-message-%d", i)),
			})
			if err != nil {
				b.Errorf("发送失败: %v", err)
			}
			i++
		}
	})

	// 输出统计信息
	stats := c.GetStats()
	b.Logf("基准测试统计: 写请求=%d, 读请求=%d, 集群模式=%v",
		stats.WriteRequests, stats.ReadRequests, stats.IsClusterMode)
}
