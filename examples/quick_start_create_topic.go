package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Go Queue CreateTopic 快速入门 ===")

	// 1. 创建客户端 - 支持多个 broker 地址
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{
			"localhost:9092",
			"localhost:9093",
			"localhost:9094",
		},
		Timeout: 5 * time.Second,
	})

	// 2. 自动发现 Controller Leader
	fmt.Println("\n步骤1: 发现 Controller Leader")
	if err := c.DiscoverController(); err != nil {
		log.Fatalf("❌ 发现 Controller 失败: %v", err)
	}

	controllerAddr := c.GetControllerAddr()
	if controllerAddr != "" {
		fmt.Printf("✓ Controller Leader: %s\n", controllerAddr)
	} else {
		fmt.Println("⚠️  未发现 Controller Leader，将使用第一个可用 Broker")
	}

	// 3. 创建管理客户端
	admin := client.NewAdmin(c)

	// 4. 创建单个主题
	fmt.Println("\n步骤2: 创建单个主题")
	result, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "my-first-topic",
		Partitions: 1,
		Replicas:   1,
	})

	if err != nil {
		log.Printf("❌ 创建主题失败: %v", err)
	} else if result.Error != nil {
		log.Printf("❌ 主题创建错误: %v", result.Error)
	} else {
		fmt.Printf("✓ 成功创建主题: %s\n", result.Name)
	}

	// 5. 创建多分区主题
	fmt.Println("\n步骤3: 创建多分区主题")
	result2, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "multi-partition-topic",
		Partitions: 3,
		Replicas:   1,
	})

	if err != nil {
		log.Printf("❌ 创建多分区主题失败: %v", err)
	} else if result2.Error != nil {
		log.Printf("❌ 多分区主题创建错误: %v", result2.Error)
	} else {
		fmt.Printf("✓ 成功创建多分区主题: %s (分区数: 3)\n", result2.Name)
	}

	// 6. 列出所有主题
	fmt.Println("\n步骤4: 列出所有主题")
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("❌ 列出主题失败: %v", err)
	} else {
		fmt.Printf("✓ 发现 %d 个主题:\n", len(topics))
	}

	fmt.Println("\n🎉 快速入门完成！")
	fmt.Println("\n💡 提示:")
	fmt.Println("   • 客户端自动发现 Controller Leader")
	fmt.Println("   • 元数据操作直接发送到 Controller")
	fmt.Println("   • 支持 Controller 故障转移")
}
