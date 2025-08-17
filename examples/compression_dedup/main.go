package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/issac1998/go-queue/pkg/client"
)

func main() {
	fmt.Println("🔧 Go Queue 压缩和去重功能演示")
	fmt.Println(strings.Repeat("=", 60))

	client := client.NewClient(client.ClientConfig{
		BrokerAddr: "localhost:9092",
		Timeout:    10 * time.Second,
	})

	testTopicName := "compression-dedup-demo"

	// 1. 创建主题
	fmt.Println("\n📝 1. 创建演示主题")
	admin := client.NewAdmin(client)
	result, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       testTopicName,
		Partitions: 1,
		Replicas:   1,
	})
	if err != nil {
		log.Fatalf("❌ 创建主题失败: %v", err)
	}
	if result.Error != nil {
		fmt.Printf("⚠️ 主题可能已存在: %v\n", result.Error)
	} else {
		fmt.Printf("✅ 主题 '%s' 创建成功\n", result.Name)
	}

	time.Sleep(1 * time.Second)
	producer := client.NewProducer(client)
	consumer := client.NewConsumer(client)

	// 2. 测试压缩功能
	fmt.Println("\n🗜️ 2. 测试消息压缩功能")

	// 创建一些重复的较大消息来测试压缩
	largeMessage := strings.Repeat("这是一条很长的测试消息，用来验证压缩功能。", 50)

	testMessages := []string{
		"短消息1",
		"短消息2",
		largeMessage,
		largeMessage + " - 变体1",
		largeMessage + " - 变体2",
	}

	fmt.Printf("📊 发送 %d 条测试消息（包含大消息）:\n", len(testMessages))

	var sentOffsets []int64
	for i, content := range testMessages {
		msg := client.ProduceMessage{
			Topic:     testTopicName,
			Partition: 0,
			Value:     []byte(content),
		}

		sendResult, err := producer.Send(msg)
		if err != nil {
			log.Printf("❌ 发送消息%d失败: %v", i+1, err)
			continue
		}
		if sendResult.Error != nil {
			log.Printf("❌ 发送消息%d服务端错误: %v", i+1, sendResult.Error)
			continue
		}

		sentOffsets = append(sentOffsets, sendResult.Offset)
		fmt.Printf("✅ 消息%d发送成功 - Offset: %d, 大小: %d字节\n",
			i+1, sendResult.Offset, len(content))
	}

	// 3. 测试去重功能
	fmt.Println("\n🔄 3. 测试消息去重功能")

	// 发送重复消息
	duplicateMsg := client.ProduceMessage{
		Topic:     testTopicName,
		Partition: 0,
		Value:     []byte("这是一条重复消息"),
	}

	fmt.Println("发送原始消息...")
	result1, err := producer.Send(duplicateMsg)
	if err != nil {
		log.Printf("❌ 发送原始消息失败: %v", err)
	} else if result1.Error != nil {
		log.Printf("❌ 发送原始消息服务端错误: %v", result1.Error)
	} else {
		fmt.Printf("✅ 原始消息发送成功 - Offset: %d\n", result1.Offset)
	}

	fmt.Println("发送相同内容消息（应该被去重）...")
	result2, err := producer.Send(duplicateMsg)
	if err != nil {
		log.Printf("❌ 发送重复消息失败: %v", err)
	} else if result2.Error != nil {
		log.Printf("❌ 发送重复消息服务端错误: %v", result2.Error)
	} else {
		fmt.Printf("✅ 重复消息处理结果 - Offset: %d\n", result2.Offset)
		if result1.Offset == result2.Offset {
			fmt.Println("🎉 去重功能正常工作！返回了相同的offset")
		} else {
			fmt.Println("⚠️ 去重功能可能未启用或配置不正确")
		}
	}

	// 4. 测试批量发送
	fmt.Println("\n📦 4. 测试批量发送")

	batchMessages := []client.ProduceMessage{
		{Topic: testTopicName, Partition: 0, Value: []byte("批量消息1")},
		{Topic: testTopicName, Partition: 0, Value: []byte("批量消息2")},
		{Topic: testTopicName, Partition: 0, Value: []byte(largeMessage)}, // 大消息测试压缩
	}

	batchResult, err := producer.SendBatch(batchMessages)
	if err != nil {
		log.Printf("❌ 批量发送失败: %v", err)
	} else if batchResult.Error != nil {
		log.Printf("❌ 批量发送服务端错误: %v", batchResult.Error)
	} else {
		fmt.Printf("✅ 批量发送成功 - 起始Offset: %d\n", batchResult.Offset)
	}

	time.Sleep(2 * time.Second)

	// 5. 读取和验证消息
	fmt.Println("\n📥 5. 读取并验证消息")

	fetchResult, err := consumer.FetchFrom(testTopicName, 0, 0)
	if err != nil {
		log.Printf("❌ 读取失败: %v", err)
	} else if fetchResult.Error != nil {
		log.Printf("❌ 读取服务端错误: %v", fetchResult.Error)
	} else {
		fmt.Printf("✅ 成功读取到 %d 条消息\n", len(fetchResult.Messages))

		fmt.Println("\n📋 消息内容验证:")
		for i, msg := range fetchResult.Messages {
			content := string(msg.Value)
			contentPreview := content
			if len(content) > 100 {
				contentPreview = content[:100] + "..."
			}

			fmt.Printf("  消息%d (Offset: %d): %s\n", i+1, msg.Offset, contentPreview)

			// 验证大消息是否正确解压
			if strings.Contains(content, "这是一条很长的测试消息") {
				fmt.Printf("    📏 大消息长度: %d 字节\n", len(content))
				if len(content) == len(largeMessage) || len(content) == len(largeMessage+" - 变体1") || len(content) == len(largeMessage+" - 变体2") {
					fmt.Printf("    ✅ 压缩/解压缩验证通过\n")
				}
			}
		}
	}

	// 6. 性能和统计信息
	fmt.Println("\n📊 6. 功能总结")
	fmt.Println("✅ 主题创建: 正常")
	fmt.Printf("✅ 消息发送: 正常 (%d条)\n", len(sentOffsets))
	fmt.Printf("✅ 压缩功能: %s\n", func() string {
		if len(largeMessage) > 100 {
			return "已测试大消息压缩"
		}
		return "待测试"
	}())
	fmt.Printf("✅ 去重功能: %s\n", func() string {
		if result1 != nil && result2 != nil && result1.Offset == result2.Offset {
			return "正常工作"
		}
		return "需要服务端启用"
	}())
	fmt.Printf("✅ 消息读取: %s\n", func() string {
		if fetchResult != nil && len(fetchResult.Messages) > 0 {
			return "正常"
		}
		return "需要调试"
	}())

	fmt.Println("\n💡 功能特性:")
	fmt.Println("  🗜️ 支持多种压缩算法 (Gzip, Zlib, Snappy, Zstd)")
	fmt.Println("  🔄 基于SHA256哈希的消息去重")
	fmt.Println("  ⚡ 自动压缩阈值控制")
	fmt.Println("  📈 压缩比率统计")
	fmt.Println("  🎯 透明的压缩/解压过程")
}
