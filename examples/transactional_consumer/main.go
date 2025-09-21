package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	// 创建客户端
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:19999"},
		Timeout:     5 * time.Second,
	})

	// 创建事务性消费者配置
	config := client.TransactionalConsumerConfig{
		GroupConsumerConfig: client.GroupConsumerConfig{
			GroupID:        "example-group",
			ConsumerID:     "consumer-1",
			Topics:         []string{"test-topic"},
			SessionTimeout: 30 * time.Second,
		},
		TransactionTimeout: 30 * time.Second,
		BatchSize:         10,
		EnableIdempotent:  true,
		IdempotentStorageConfig: client.IdempotentStorageConfig{
			StorageType:     "memory",
			MaxRecords:      1000,
			CleanupInterval: 5 * time.Minute,
		},
	}

	// 创建事务性消费者
	consumer, err := client.NewTransactionalConsumer(c, config)
	if err != nil {
		log.Fatalf("Failed to create transactional consumer: %v", err)
	}
	defer consumer.Close()

	log.Println("Starting transactional consumer with exact-once semantics...")

	// 加入消费者组
	err = consumer.JoinGroup()
	if err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}
	defer consumer.LeaveGroup()

	// 订阅主题
	err = consumer.Subscribe([]string{"test-topic"})
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
	}

	// 使用事务性消费
	ctx := context.Background()
	err = consumer.ConsumeTransactionally(ctx, func(messages []*client.ConsumeMessage) error {
		log.Printf("Processing batch of %d messages with exact-once semantics", len(messages))
		
		for _, msg := range messages {
			// 处理消息业务逻辑
			err := processMessage(msg)
			if err != nil {
				return fmt.Errorf("failed to process message: %v", err)
			}
		}
		
		log.Printf("Successfully processed %d messages", len(messages))
		return nil
	})
	
	if err != nil {
		log.Printf("Transactional consumption error: %v", err)
	}
}

// processMessage 模拟消息处理逻辑
func processMessage(msg *client.ConsumeMessage) error {
	log.Printf("Processing message: Topic=%s, Partition=%d, Offset=%d, Value=%s",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	
	// 模拟处理时间
	time.Sleep(10 * time.Millisecond)
	
	// 模拟偶尔的处理失败
	if string(msg.Value) == "error" {
		return fmt.Errorf("simulated processing error")
	}
	
	return nil
}