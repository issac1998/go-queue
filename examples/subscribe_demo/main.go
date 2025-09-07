package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Subscribe Functionality Demo ===")

	// Create client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	// Create topics for testing
	admin := client.NewAdmin(c)
	topics := []string{"subscribe-topic-1", "subscribe-topic-2", "subscribe-topic-3"}

	fmt.Println("1. Creating test topics...")
	for _, topic := range topics {
		result, err := admin.CreateTopic(client.CreateTopicRequest{
			Name:       topic,
			Partitions: 2,
			Replicas:   1,
		})
		if err != nil {
			log.Printf("Failed to create topic %s: %v", topic, err)
		} else {
			fmt.Printf("  ✓ Created topic: %s\n", result.Name)
		}
	}

	// Produce some messages to each topic
	fmt.Println("\n2. Producing test messages...")
	producer := client.NewProducerWithStrategy(c, client.PartitionStrategyRoundRobin)

	for i, topic := range topics {
		for j := 1; j <= 3; j++ {
			message := fmt.Sprintf("Message %d from %s", j, topic)
			result, err := producer.Send(client.ProduceMessage{
				Topic:     topic,
				Partition: -1,
				Value:     []byte(message),
			})
			if err != nil {
				log.Printf("Failed to send to %s: %v", topic, err)
			} else {
				fmt.Printf("  ✓ Sent to %s (P%d, offset %d): %s\n", topic, result.Partition, result.Offset, message)
			}
		}
		if i < len(topics)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Println("\n3. Testing Regular Consumer Subscribe...")
	testRegularConsumerSubscribe(c)

	fmt.Println("\n4. Testing Consumer Group Dynamic Subscribe...")
	testConsumerGroupSubscribe(c)

	fmt.Println("\n✅ Subscribe Demo Completed!")
}

func testRegularConsumerSubscribe(c *client.Client) {
	consumer := client.NewConsumer(c)

	// Subscribe to multiple topics
	topics := []string{"subscribe-topic-1", "subscribe-topic-2"}
	fmt.Printf("  Subscribing to topics: %v\n", topics)

	err := consumer.Subscribe(topics)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
		return
	}

	// Check subscription
	subscription := consumer.GetSubscription()
	fmt.Printf("  ✓ Current subscription: %v\n", subscription)

	// Poll messages
	fmt.Println("  Polling messages...")
	messages, err := consumer.Poll(3 * time.Second)
	if err != nil {
		log.Printf("Poll error: %v", err)
		return
	}

	fmt.Printf("  📨 Received %d messages:\n", len(messages))
	for i, msg := range messages {
		if i < 5 { // Show first 5 messages
			fmt.Printf("    - %s (topic: %s, partition: %d, offset: %d)\n",
				string(msg.Value), msg.Topic, msg.Partition, msg.Offset)
		} else if i == 5 {
			fmt.Printf("    - ... and %d more messages\n", len(messages)-5)
			break
		}
	}

	// Subscribe to another topic
	fmt.Println("  Adding subscription to subscribe-topic-3...")
	err = consumer.Subscribe([]string{"subscribe-topic-3"})
	if err != nil {
		log.Printf("Failed to add subscription: %v", err)
		return
	}

	subscription = consumer.GetSubscription()
	fmt.Printf("  ✓ Updated subscription: %v\n", subscription)

	// Unsubscribe from one topic
	fmt.Println("  Unsubscribing from subscribe-topic-1...")
	err = consumer.Unsubscribe([]string{"subscribe-topic-1"})
	if err != nil {
		log.Printf("Failed to unsubscribe: %v", err)
		return
	}

	subscription = consumer.GetSubscription()
	fmt.Printf("  ✓ Final subscription: %v\n", subscription)
}

func testConsumerGroupSubscribe(c *client.Client) {
	// Create consumer group with initial topics
	consumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        "subscribe-demo-group",
		ConsumerID:     "subscriber-1",
		Topics:         []string{"subscribe-topic-1"},
		SessionTimeout: 30 * time.Second,
	})

	// Join group
	fmt.Println("  Consumer joining group...")
	err := consumer.JoinGroup()
	if err != nil {
		log.Printf("Failed to join group: %v", err)
		return
	}
	defer consumer.LeaveGroup()

	// Check initial subscription
	subscription := consumer.GetSubscription()
	fmt.Printf("  ✓ Initial subscription: %v\n", subscription)

	assignment := consumer.GetAssignment()
	fmt.Printf("  ✓ Initial assignment: %v\n", assignment)

	// Dynamically subscribe to more topics
	fmt.Println("  Adding dynamic subscription to more topics...")
	err = consumer.Subscribe([]string{"subscribe-topic-2", "subscribe-topic-3"})
	if err != nil {
		log.Printf("Failed to add subscription: %v", err)
		return
	}

	subscription = consumer.GetSubscription()
	fmt.Printf("  ✓ Updated subscription: %v\n", subscription)

	// Check if subscribed
	for _, topic := range []string{"subscribe-topic-1", "subscribe-topic-2", "subscribe-topic-3"} {
		isSubscribed := consumer.IsSubscribed(topic)
		fmt.Printf("    - %s: subscribed=%t\n", topic, isSubscribed)
	}

	// Test unsubscribe
	fmt.Println("  Unsubscribing from subscribe-topic-1...")
	err = consumer.Unsubscribe([]string{"subscribe-topic-1"})
	if err != nil {
		log.Printf("Failed to unsubscribe: %v", err)
		return
	}

	subscription = consumer.GetSubscription()
	fmt.Printf("  ✓ Final subscription: %v\n", subscription)

	// Poll some messages from current assignment
	fmt.Println("  Polling messages from current assignment...")
	messages, err := consumer.Poll(2 * time.Second)
	if err != nil {
		log.Printf("Poll error: %v", err)
		return
	}

	fmt.Printf("  📨 Received %d messages from assigned partitions\n", len(messages))
	for i, msg := range messages {
		if i < 3 {
			fmt.Printf("    - %s (topic: %s, partition: %d)\n",
				string(msg.Value), msg.Topic, msg.Partition)
		} else if i == 3 {
			fmt.Printf("    - ... and %d more\n", len(messages)-3)
			break
		}
	}
}
