package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	// Create client
	c := client.NewClient(client.ClientConfig{
		BrokerAddr: "localhost:9092",
		Timeout:    5 * time.Second,
	})

	fmt.Println("=== Go Queue Client SDK Example ===")

	// 1. Create topic
	fmt.Println("\n1. Creating topic...")
	admin := client.NewAdmin(c)
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "test-topic",
		Partitions: 3,
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
	} else if createResult.Error != nil {
		log.Printf("Failed to create topic: %v", createResult.Error)
	} else {
		fmt.Printf("Topic '%s' created successfully!\n", createResult.Name)
	}

	// 2. Send messages
	fmt.Println("\n2. Sending messages...")
	producer := client.NewProducer(c)

	// Send single message
	msg := client.ProduceMessage{
		Topic:     "test-topic",
		Partition: 0,
		Value:     []byte("Hello, Go Queue!"),
	}

	result, err := producer.Send(msg)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else if result.Error != nil {
		log.Printf("Failed to send message: %v", result.Error)
	} else {
		fmt.Printf("Message sent successfully! Offset: %d\n", result.Offset)
	}

	// Send batch messages
	messages := []client.ProduceMessage{
		{Topic: "test-topic", Partition: 0, Value: []byte("Message 1")},
		{Topic: "test-topic", Partition: 0, Value: []byte("Message 2")},
		{Topic: "test-topic", Partition: 0, Value: []byte("Message 3")},
	}

	batchResult, err := producer.SendBatch(messages)
	if err != nil {
		log.Printf("Failed to send batch messages: %v", err)
	} else if batchResult.Error != nil {
		log.Printf("Failed to send batch messages: %v", batchResult.Error)
	} else {
		fmt.Printf("Batch messages sent successfully! Start Offset: %d\n", batchResult.Offset)
	}

	// 3. Consume messages
	fmt.Println("\n3. Consuming messages...")
	consumer := client.NewConsumer(c)

	// Fetch messages from offset 0
	fetchResult, err := consumer.FetchFrom("test-topic", 0, 0)
	if err != nil {
		log.Printf("Failed to fetch messages: %v", err)
	} else if fetchResult.Error != nil {
		log.Printf("Failed to fetch messages: %v", fetchResult.Error)
	} else {
		fmt.Printf("Successfully fetched %d messages:\n", len(fetchResult.Messages))
		for i, msg := range fetchResult.Messages {
			fmt.Printf("  Message %d: %s (Offset: %d)\n", i+1, string(msg.Value), msg.Offset)
		}
		fmt.Printf("Next Offset: %d\n", fetchResult.NextOffset)
	}

	// 4. Subscribe messages (demonstrate Subscribe method)
	fmt.Println("\n4. Subscribe mode consumption (starting from latest position)...")

	// Send a few new messages for demonstration
	for i := 0; i < 3; i++ {
		msg := client.ProduceMessage{
			Topic:     "test-topic",
			Partition: 0,
			Value:     []byte(fmt.Sprintf("Subscribe test message %d", i+1)),
		}
		producer.Send(msg)
	}

	// Message handler for subscription
	messageHandler := func(msg client.Message) error {
		fmt.Printf("  Received subscribed message: %s (Offset: %d)\n", string(msg.Value), msg.Offset)
		return nil
	}

	// Note: Subscribe is blocking, in actual applications it should run in a goroutine
	err = consumer.Subscribe("test-topic", 0, messageHandler)
	if err != nil {
		log.Printf("Failed to subscribe to messages: %v", err)
	}

	fmt.Println("\n=== Example completed ===")
}
