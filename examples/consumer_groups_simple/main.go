package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Simple Consumer Group Test ===")

	// Create a client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	fmt.Println("\n1. Creating test topic...")
	admin := client.NewAdmin(c)
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "test-consumer-group",
		Partitions: 3,
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
	} else if createResult.Error != nil {
		log.Printf("Topic creation error: %v", createResult.Error)
	} else {
		fmt.Printf("✓ Topic '%s' created successfully with 3 partitions!\n", createResult.Name)
	}

	fmt.Println("\n2. Producing test messages...")
	producer := client.NewProducer(c)

	for i := 0; i < 9; i++ {
		partition := int32(i % 3) // Distribute across 3 partitions
		message := fmt.Sprintf("Test message %d", i+1)

		result, err := producer.Send(client.ProduceMessage{
			Topic:     "test-consumer-group",
			Partition: partition,
			Value:     []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message %d: %v", i+1, err)
			continue
		}
		if result.Error != nil {
			log.Printf("Message %d send error: %v", i+1, result.Error)
			continue
		}

		fmt.Printf("  ✓ Sent '%s' to partition %d (offset: %d)\n", message, partition, result.Offset)
	}

	fmt.Println("\n3. Creating consumer group...")

	// Create consumer group consumer
	groupConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        "test-group",
		ConsumerID:     "test-consumer-1",
		Topics:         []string{"test-consumer-group"},
		SessionTimeout: 30 * time.Second,
	})

	fmt.Println("  Joining consumer group...")
	err = groupConsumer.JoinGroup()
	if err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}

	fmt.Println("  ✓ Successfully joined consumer group!")

	// Check assignment
	assignment := groupConsumer.GetAssignment()
	fmt.Printf("  Assignment: %v\n", assignment)

	fmt.Println("\n4. Testing offset operations...")

	// Commit some offsets
	err = groupConsumer.CommitOffset("test-consumer-group", 0, 2, "test metadata")
	if err != nil {
		log.Printf("Failed to commit offset: %v", err)
	} else {
		fmt.Println("  ✓ Committed offset for partition 0")
	}

	// Fetch committed offset
	offset, err := groupConsumer.FetchCommittedOffset("test-consumer-group", 0)
	if err != nil {
		log.Printf("Failed to fetch offset: %v", err)
	} else {
		fmt.Printf("  ✓ Fetched committed offset: %d\n", offset)
	}

	fmt.Println("\n5. Testing heartbeat...")
	time.Sleep(2 * time.Second) // Let heartbeat work in background

	fmt.Println("\n6. Leaving consumer group...")
	err = groupConsumer.LeaveGroup()
	if err != nil {
		log.Printf("Failed to leave group: %v", err)
	} else {
		fmt.Println("  ✓ Successfully left consumer group!")
	}

	fmt.Println("\n✓ Consumer group test completed successfully!")
}
