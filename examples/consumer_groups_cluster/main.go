package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	// 连接到集群中的多个broker
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Timeout:     10 * time.Second,
	})

	// 创建topic (如果不存在)
	err := c.CreateTopic("cluster-test-topic", 3, 2) // 3个分区，2个副本
	if err != nil {
		log.Printf("Failed to create topic (may already exist): %v", err)
	}

	// 创建消费者组
	groupConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        "cluster-consumer-group-1",
		ConsumerID:     "consumer-1",
		Topics:         []string{"cluster-test-topic"},
		SessionTimeout: 30 * time.Second,
	})

	// 加入消费者组
	err = groupConsumer.JoinGroup()
	if err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}
	log.Printf("Successfully joined consumer group in cluster mode")

	// 启动心跳
	err = groupConsumer.StartHeartbeat()
	if err != nil {
		log.Fatalf("Failed to start heartbeat: %v", err)
	}
	log.Printf("Heartbeat started")

	// 模拟生产一些消息
	go func() {
		for i := 0; i < 10; i++ {
			message := fmt.Sprintf("Cluster message %d from producer", i)
			err := c.Produce("cluster-test-topic", 0, []byte(message))
			if err != nil {
				log.Printf("Failed to produce message: %v", err)
			} else {
				log.Printf("Produced: %s", message)
			}
			time.Sleep(2 * time.Second)
		}
	}()

	// 消费消息
	log.Printf("Starting to consume messages from cluster...")
	for i := 0; i < 10; i++ {
		// 订阅消息
		messages, err := groupConsumer.Subscribe()
		if err != nil {
			log.Printf("Failed to subscribe: %v", err)
			continue
		}

		for _, msg := range messages {
			log.Printf("Consumed from cluster: Topic=%s, Partition=%d, Offset=%d, Message=%s",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

			// 提交offset (使用4参数版本)
			err = groupConsumer.CommitOffset(msg.Topic, msg.Partition, msg.Offset+1, "")
			if err != nil {
				log.Printf("Failed to commit offset: %v", err)
			}
		}

		time.Sleep(1 * time.Second)
	}

	// 离开消费者组
	err = groupConsumer.LeaveGroup()
	if err != nil {
		log.Printf("Failed to leave group: %v", err)
	} else {
		log.Printf("Successfully left consumer group")
	}

	// 停止心跳
	groupConsumer.StopHeartbeat()
	log.Printf("Heartbeat stopped")

	log.Printf("Cluster consumer group example completed")
}
