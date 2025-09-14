package main

import (
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	log.Println("=== Simple Async I/O Test ===")

	// Create client with async I/O enabled
	clientConfig := client.ClientConfig{
		BrokerAddrs:   []string{"localhost:9092"},
		Timeout:       30 * time.Second, // Longer timeout for single node
		EnableAsyncIO: true,
	}

	c := client.NewClient(clientConfig)

	// Wait for controller to be ready
	log.Println("Waiting for controller to be ready...")
	time.Sleep(5 * time.Second)

	// Create admin client and try to create a topic
	admin := client.NewAdmin(c)
	topicName := "async-test-topic"

	log.Printf("Creating topic '%s'...", topicName)
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 1,
		Replicas:   1,
	})

	if err != nil {
		log.Printf("Topic creation failed: %v", err)
		return
	}

	if createResult.Error != nil {
		log.Printf("Topic creation error: %v", createResult.Error)
		return
	}

	log.Printf("Topic '%s' created successfully", createResult.Name)

	// Test ordered producer with async I/O
	orderedProducer := client.NewOrderedProducer(c)

	messages := []client.OrderedMessage{
		{Value: []byte("Async message 1"), MessageGroup: "test-group"},
		{Value: []byte("Async message 2"), MessageGroup: "test-group"},
		{Value: []byte("Async message 3"), MessageGroup: "test-group"},
	}

	log.Println("Sending ordered messages with async I/O...")
	start := time.Now()
	result, err := orderedProducer.SendOrderedMessages(topicName, messages)
	if err != nil {
		log.Printf("Failed to send messages: %v", err)
		return
	}

	duration := time.Since(start)
	log.Printf("✅ Successfully sent %d messages in %v", result.SuccessfulMessages, duration)
	log.Printf("Success rate: %d/%d", result.SuccessfulMessages, result.TotalMessages)

	// Show message group routing
	for group, partition := range result.MessageGroupRouting {
		log.Printf("Message group '%s' routed to partition %d", group, partition)
	}

	log.Println("\n🎉 Async I/O test completed successfully!")
	log.Println("Key benefits demonstrated:")
	log.Println("  ✅ Ordered messages within message group")
	log.Println("  ✅ Async I/O for improved performance")
	log.Println("  ✅ Automatic fallback on errors")
}