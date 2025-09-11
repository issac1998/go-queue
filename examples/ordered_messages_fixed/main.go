package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Go Queue 顺序消息功能演示 (修复版) ===")

	// 创建客户端配置
	config := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	}

	// 创建客户端
	queueClient := client.NewClient(config)

	// 创建顺序消息生产者
	orderedProducer := client.NewOrderedProducer(queueClient)

	fmt.Println("\n1. 演示MessageGroup分区选择...")

	// 准备不同MessageGroup的消息
	messages := []client.OrderedMessage{
		{
			Key:          []byte("order-001"),
			Value:        []byte(`{"event":"order_created","user_id":"123","order_id":"001"}`),
			MessageGroup: "user-123", // 用户123的消息组
		},
		{
			Key:          []byte("order-002"),
			Value:        []byte(`{"event":"order_paid","user_id":"123","order_id":"002"}`),
			MessageGroup: "user-123", // 同一用户，应该路由到相同分区
		},
		{
			Key:          []byte("order-003"),
			Value:        []byte(`{"event":"order_created","user_id":"456","order_id":"003"}`),
			MessageGroup: "user-456", // 不同用户，可能路由到不同分区
		},
		{
			Key:          []byte("order-004"),
			Value:        []byte(`{"event":"order_shipped","user_id":"789","order_id":"004"}`),
			MessageGroup: "user-789", // 另一个用户
		},
	}

	fmt.Printf("准备发送 %d 条消息，包含 3 个不同的MessageGroup...\n", len(messages))

	// 显示MessageGroup到分区的映射（模拟）
	fmt.Println("\nMessageGroup分区映射（模拟4个分区）:")
	partitioner := client.NewOrderedPartitioner()
	for _, msg := range messages {
		partition := partitioner.SelectPartitionForMessageGroup(msg.MessageGroup, 4)
		fmt.Printf("  MessageGroup '%s' → 分区 %d\n", msg.MessageGroup, partition)
	}

	// 验证相同MessageGroup总是路由到相同分区
	fmt.Println("\n2. 验证分区一致性...")
	userGroup := "user-123"
	partition1 := partitioner.SelectPartitionForMessageGroup(userGroup, 4)
	partition2 := partitioner.SelectPartitionForMessageGroup(userGroup, 4)
	partition3 := partitioner.SelectPartitionForMessageGroup(userGroup, 4)

	fmt.Printf("MessageGroup '%s' 多次调用结果: %d, %d, %d", userGroup, partition1, partition2, partition3)
	if partition1 == partition2 && partition2 == partition3 {
		fmt.Println(" ✅ 一致性验证通过")
	} else {
		fmt.Println(" ❌ 一致性验证失败")
	}

	// 尝试发送消息（注意：实际发送需要运行的broker服务）
	fmt.Println("\n3. 尝试发送顺序消息...")
	result, err := orderedProducer.SendOrderedMessages("order-events", messages)
	if err != nil {
		log.Printf("发送失败: %v (这是正常的，因为没有运行broker服务)", err)
	} else {
		fmt.Printf("✅ 发送成功: %d/%d 条消息\n", result.SuccessfulMessages, result.TotalMessages)
		fmt.Printf("涉及分区: %v\n", result.MessageGroupRouting)
	}

	fmt.Println("\n=== 修复说明 ===")
	fmt.Println("✅ 问题: 原来传递-1给connectForDataOperation导致分区查找失败")
	fmt.Println("✅ 解决: 实现OrderedPartitioner在客户端进行MessageGroup分区选择")
	fmt.Println("✅ 特性: 相同MessageGroup总是路由到相同分区，保证顺序")
	fmt.Println("✅ 特性: 不同MessageGroup自动负载均衡到不同分区")
	fmt.Println("✅ 特性: 支持跨分区的批量消息发送")

	fmt.Println("\n演示完成！")
}
