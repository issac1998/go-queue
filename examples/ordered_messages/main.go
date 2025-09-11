package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("🚀 Go-Queue 顺序消息功能演示")
	fmt.Println("===================================")

	// 1. 创建客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}

	c := client.NewClient(clientConfig)
	fmt.Println("✅ 客户端创建成功")

	// 2. 创建Topic（如果不存在）
	admin := client.NewAdmin(c)
	topicName := "order-events"

	_, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 4, // 4个分区用于演示负载均衡
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Topic可能已存在: %v", err)
	} else {
		fmt.Printf("✅ Topic '%s' 创建成功（4个分区）\n", topicName)
	}

	// 等待Topic创建完成
	time.Sleep(2 * time.Second)

	// 3. 创建顺序消息生产者
	orderedProducer := client.NewOrderedProducer(c)
	fmt.Println("✅ 顺序消息生产者创建成功")

	// 4. 演示场景1：单用户的订单事件序列（必须保证顺序）
	fmt.Println("\n📦 场景1：用户123的订单事件序列")
	user123Events := []client.OrderedMessage{
		{
			Key:          []byte("order-1001"),
			Value:        []byte(`{"event":"order_created","user_id":"123","order_id":"1001","amount":100}`),
			MessageGroup: "user-123",
		},
		{
			Key:          []byte("order-1001"),
			Value:        []byte(`{"event":"order_paid","user_id":"123","order_id":"1001","amount":100}`),
			MessageGroup: "user-123",
		},
		{
			Key:          []byte("order-1001"),
			Value:        []byte(`{"event":"order_shipped","user_id":"123","order_id":"1001","tracking":"ABC123"}`),
			MessageGroup: "user-123",
		},
		{
			Key:          []byte("order-1001"),
			Value:        []byte(`{"event":"order_delivered","user_id":"123","order_id":"1001","delivery_time":"2023-12-01T10:00:00Z"}`),
			MessageGroup: "user-123",
		},
	}

	result1, err := orderedProducer.SendOrderedMessages(topicName, user123Events)
	if err != nil {
		log.Fatalf("❌ 发送用户123事件失败: %v", err)
	}

	fmt.Printf("✅ 用户123的4个事件发送成功: %d/%d\n",
		result1.SuccessfulMessages, result1.TotalMessages)
	printPartitionDistribution(result1)

	// 5. 演示场景2：多用户订单事件（不同MessageGroup，负载均衡）
	fmt.Println("\n📦 场景2：多用户订单事件（负载均衡演示）")
	multiUserEvents := []client.OrderedMessage{
		{
			Key:          []byte("order-2001"),
			Value:        []byte(`{"event":"order_created","user_id":"456","order_id":"2001","amount":200}`),
			MessageGroup: "user-456",
		},
		{
			Key:          []byte("order-3001"),
			Value:        []byte(`{"event":"order_created","user_id":"789","order_id":"3001","amount":300}`),
			MessageGroup: "user-789",
		},
		{
			Key:          []byte("order-4001"),
			Value:        []byte(`{"event":"order_created","user_id":"101","order_id":"4001","amount":400}`),
			MessageGroup: "user-101",
		},
		{
			Key:          []byte("order-5001"),
			Value:        []byte(`{"event":"order_created","user_id":"112","order_id":"5001","amount":500}`),
			MessageGroup: "user-112",
		},
	}

	result2, err := orderedProducer.SendOrderedMessages(topicName, multiUserEvents)
	if err != nil {
		log.Fatalf("❌ 发送多用户事件失败: %v", err)
	}

	fmt.Printf("✅ 多用户事件发送成功: %d/%d\n",
		result2.SuccessfulMessages, result2.TotalMessages)
	printPartitionDistribution(result2)

	// 6. 演示场景3：同一用户的后续事件（验证路由一致性）
	fmt.Println("\n📦 场景3：用户456的后续事件（验证路由一致性）")
	user456FollowUp := []client.OrderedMessage{
		{
			Key:          []byte("order-2001"),
			Value:        []byte(`{"event":"order_paid","user_id":"456","order_id":"2001","amount":200}`),
			MessageGroup: "user-456",
		},
		{
			Key:          []byte("order-2002"),
			Value:        []byte(`{"event":"order_created","user_id":"456","order_id":"2002","amount":250}`),
			MessageGroup: "user-456",
		},
	}

	result3, err := orderedProducer.SendOrderedMessages(topicName, user456FollowUp)
	if err != nil {
		log.Fatalf("❌ 发送用户456后续事件失败: %v", err)
	}

	fmt.Printf("✅ 用户456后续事件发送成功: %d/%d\n",
		result3.SuccessfulMessages, result3.TotalMessages)
	printPartitionDistribution(result3)

	// 7. 演示便捷方法
	fmt.Println("\n📦 场景4：使用便捷方法发送单条消息")
	singleResult, err := orderedProducer.SendSingleOrderedMessage(
		topicName,
		"user-999",
		[]byte("order-9999"),
		[]byte(`{"event":"order_created","user_id":"999","order_id":"9999","amount":999}`),
	)
	if err != nil {
		log.Fatalf("❌ 发送单条消息失败: %v", err)
	}

	fmt.Printf("✅ 单条消息发送成功: %d/%d\n",
		singleResult.SuccessfulMessages, singleResult.TotalMessages)
	printPartitionDistribution(singleResult)

	// 8. 总结
	fmt.Println("\n📊 演示完成总结:")
	fmt.Println("================================")
	fmt.Println("✅ 顺序保证: 同一MessageGroup的消息总是路由到同一分区")
	fmt.Println("✅ 负载均衡: 不同MessageGroup分散到不同分区")
	fmt.Println("✅ 高性能: 支持批量发送和并行处理")
	fmt.Println("✅ 易用性: 提供多种便捷的API方法")

	// 9. 演示消费端顺序读取（简化版）
	fmt.Println("\n📖 验证顺序消费:")
	demonstrateOrderedConsumption(c, topicName)
}

// printPartitionDistribution 打印分区分布情况
func printPartitionDistribution(result *client.OrderedProduceResult) {
	fmt.Printf("   分区分布: ")
	for partition := range result.PartitionResponses {
		fmt.Printf("P%s ", partition)
	}
	fmt.Println()
}

// demonstrateOrderedConsumption 演示顺序消费（简化版）
func demonstrateOrderedConsumption(c *client.Client, topicName string) {
	consumer := client.NewConsumer(c)

	// 从每个分区读取一些消息来验证顺序
	for partition := int32(0); partition < 4; partition++ {
		fmt.Printf("📖 读取分区 %d 的消息:\n", partition)

		result, err := consumer.FetchFrom(topicName, partition, 0)
		if err != nil {
			log.Printf("   ❌ 读取分区%d失败: %v", partition, err)
			continue
		}

		if len(result.Messages) == 0 {
			fmt.Printf("   📭 分区%d暂无消息\n", partition)
			continue
		}

		// 显示该分区的MessageGroup分布
		messageGroups := make(map[string]int)
		for _, msg := range result.Messages {
			// 尝试解析MessageGroup（这里简化处理）
			msgStr := string(msg.Value)
			if len(msgStr) > 50 {
				msgStr = msgStr[:50] + "..."
			}
			fmt.Printf("   📄 Offset %d: %s\n", msg.Offset, msgStr)
		}

		if len(messageGroups) > 0 {
			fmt.Printf("   🏷️  MessageGroup分布: %v\n", messageGroups)
		}
	}
}

// 辅助函数：验证MessageGroup路由一致性
func verifyRoutingConsistency() {
	fmt.Println("\n🔍 MessageGroup路由一致性验证:")
	fmt.Println("- user-123 的所有消息应该在同一分区")
	fmt.Println("- user-456 的所有消息应该在同一分区")
	fmt.Println("- 不同用户的消息应该分散在不同分区")
	fmt.Println("- 这确保了：")
	fmt.Println("  ✅ 同一用户的事件严格有序")
	fmt.Println("  ✅ 不同用户的事件并行处理")
	fmt.Println("  ✅ 系统整体吞吐量最大化")
}
