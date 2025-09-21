package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	// Create client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:19999"},
		Timeout:     5 * time.Second,
	})

	// Create transactional consumer configuration
	config := client.TransactionalConsumerConfig{
		GroupConsumerConfig: client.GroupConsumerConfig{
			GroupID:        "example-group",
			ConsumerID:     "consumer-1",
			Topics:         []string{"test-topic"},
			SessionTimeout: 30 * time.Second,
		},
		TransactionTimeout: 30 * time.Second,
		BatchSize:          10,
		EnableIdempotent:   true,
		IdempotentStorageConfig: client.IdempotentStorageConfig{
			StorageType:     "memory",
			MaxRecords:      1000,
			CleanupInterval: 5 * time.Minute,
		},
	}

	// Create transactional consumer
	consumer, err := client.NewTransactionalConsumer(c, config)
	if err != nil {
		log.Fatalf("Failed to create transactional consumer: %v", err)
	}
	defer consumer.Close()

	log.Println("Starting transactional consumer with exact-once semantics...")

	// Join consumer group
	err = consumer.JoinGroup()
	if err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}
	defer consumer.LeaveGroup()

	// Subscribe to topics
	err = consumer.Subscribe([]string{"test-topic"})
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
	}

	// Use transactional consumption
	ctx := context.Background()
	err = consumer.ConsumeTransactionally(ctx, func(msg *client.ConsumeMessage) error {

		// Process message business logic
		err := processMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to process message: %v", err)
		}

		return nil
	}, func(msg *client.ConsumeMessage) error {
		// Commit message offset
		return nil
	}, func(msg *client.ConsumeMessage) error {
		// Rollback message offset
		return nil
	})

	if err != nil {
		log.Printf("Transactional consumption error: %v", err)
	}
}

// processMessage simulates message processing logic
func processMessage(msg *client.ConsumeMessage) error {
	log.Printf("Processing message: Topic=%s, Partition=%d, Offset=%d, Value=%s",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

	// Simulate processing time
	time.Sleep(10 * time.Millisecond)

	// Simulate occasional processing failure
	if string(msg.Value) == "error" {
		return fmt.Errorf("simulated processing error")
	}

	return nil
}
