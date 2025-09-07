package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Simple Multi-Consumer Demo ===")

	// Create client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	topicName := "multi-consumer-topic"

	// 1. Create topic
	fmt.Printf("Creating topic '%s'...\n", topicName)
	admin := client.NewAdmin(c)
	_, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 3,
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Topic creation may have failed: %v", err)
	}

	// 2. Produce some messages
	fmt.Println("Producing 12 messages...")
	producer := client.NewProducerWithStrategy(c, client.PartitionStrategyRoundRobin)

	for i := 1; i <= 12; i++ {
		msg := fmt.Sprintf("Message %d", i)
		result, err := producer.Send(client.ProduceMessage{
			Topic:     topicName,
			Partition: -1, // Auto-assign
			Value:     []byte(msg),
		})
		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
		} else {
			fmt.Printf("  ✓ Sent: %s (partition: %d, offset: %d)\n", msg, result.Partition, result.Offset)
		}
	}

	// 3. Start 2 consumers in the same group
	fmt.Println("\nStarting 2 consumers...")

	var wg sync.WaitGroup
	groupID := "simple-group"

	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go startConsumer(c, groupID, fmt.Sprintf("consumer-%d", i), topicName, &wg)
	}

	// Wait for consumers to process messages
	time.Sleep(10 * time.Second)

	fmt.Println("\n✅ Demo completed! Check the logs above to see how messages were distributed between consumers.")
}

func startConsumer(c *client.Client, groupID, consumerID, topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create group consumer
	consumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        groupID,
		ConsumerID:     consumerID,
		Topics:         []string{topic},
		SessionTimeout: 30 * time.Second,
	})

	// Join group
	fmt.Printf("%s joining group %s...\n", consumerID, groupID)
	if err := consumer.JoinGroup(); err != nil {
		log.Printf("%s failed to join group: %v", consumerID, err)
		return
	}
	defer consumer.LeaveGroup()

	assignment := consumer.GetAssignment()
	fmt.Printf("%s assignment: %v\n", consumerID, assignment)

	// Consume messages
	fmt.Printf("%s starting to consume...\n", consumerID)
	messagesProcessed := 0

	for messagesProcessed < 10 { // Process up to 10 messages per consumer
		messages, err := consumer.Poll(2 * time.Second)
		if err != nil {
			log.Printf("%s poll error: %v", consumerID, err)
			continue
		}

		if len(messages) == 0 {
			continue
		}

		for _, message := range messages {
			fmt.Printf("🔵 %s consumed: '%s' (partition: %d, offset: %d)\n",
				consumerID, string(message.Value), message.Partition, message.Offset)

			// Commit offset
			if err := consumer.CommitOffset(message.Topic, message.Partition, message.Offset, "processed"); err != nil {
				log.Printf("%s failed to commit offset: %v", consumerID, err)
			}

			messagesProcessed++
			if messagesProcessed >= 10 {
				break
			}
		}
	}

	fmt.Printf("✅ %s finished processing %d messages\n", consumerID, messagesProcessed)
}
