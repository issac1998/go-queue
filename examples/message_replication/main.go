package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Go Queue 消息副本同步示例 ===")

	// 创建客户端连接到集群
	c := client.NewClient(client.ClientConfig{
		BrokerAddr: "localhost:9092", // 连接到集群的第一个节点
		Timeout:    10 * time.Second,
	})

	// 创建管理员客户端
	admin := client.NewAdmin(c)

	// 创建一个带副本的主题
	fmt.Println("\n1. 创建带副本的主题...")
	topicResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "replicated-topic",
		Partitions: 3,
		Replicas:   3, // 3个副本
	})
	if err != nil {
		log.Printf("创建主题失败: %v", err)
	} else if topicResult.Error != nil {
		log.Printf("创建主题错误: %v", topicResult.Error)
	} else {
		fmt.Printf("成功创建主题: %s (3个分区，3个副本)\n", topicResult.Name)
	}

	// 等待一下让集群同步
	time.Sleep(2 * time.Second)

	// 创建生产者
	producer := client.NewProducer(c)

	// 发送消息到不同分区
	fmt.Println("\n2. 发送消息到不同分区...")
	messages := []struct {
		partition int32
		message   string
	}{
		{0, "消息1 - 分区0"},
		{1, "消息2 - 分区1"},
		{2, "消息3 - 分区2"},
		{0, "消息4 - 分区0"},
		{1, "消息5 - 分区1"},
	}

	for i, msg := range messages {
		produceMsg := client.ProduceMessage{
			Topic:     "replicated-topic",
			Partition: msg.partition,
			Value:     []byte(msg.message),
		}

		result, err := producer.Send(produceMsg)
		if err != nil {
			log.Printf("发送消息 %d 失败: %v", i+1, err)
		} else if result.Error != nil {
			log.Printf("发送消息 %d 错误: %v", i+1, result.Error)
		} else {
			fmt.Printf("消息 %d 发送成功: Offset=%d, 分区=%d\n",
				i+1, result.Offset, result.Partition)
		}

		// 稍微等待一下让副本同步
		time.Sleep(500 * time.Millisecond)
	}

	// 创建消费者
	consumer := client.NewConsumer(c)

	// 从每个分区读取消息
	fmt.Println("\n3. 从各分区读取消息...")
	for partition := int32(0); partition < 3; partition++ {
		fmt.Printf("\n--- 分区 %d ---\n", partition)

		fetchResult, err := consumer.FetchFrom("replicated-topic", partition, 0)
		if err != nil {
			log.Printf("从分区 %d 读取失败: %v", partition, err)
			continue
		}

		if fetchResult.Error != nil {
			log.Printf("从分区 %d 读取错误: %v", partition, fetchResult.Error)
			continue
		}

		if len(fetchResult.Messages) == 0 {
			fmt.Printf("分区 %d 暂无消息\n", partition)
		} else {
			for i, msg := range fetchResult.Messages {
				fmt.Printf("消息 %d: %s (Offset: %d)\n",
					i+1, string(msg.Value), msg.Offset)
			}
		}
	}

	fmt.Println("\n=== 消息副本同步示例完成 ===")
	fmt.Println("注意: 这个示例需要运行在集群模式下才能看到副本同步效果")
	fmt.Println("请确保启动了多个broker节点并配置了集群模式")
}
