package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Automatic Rebalancing Demo ===")

	// Create client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	// Create multiple topics
	admin := client.NewAdmin(c)
	topics := []string{"orders", "payments", "inventory", "notifications"}

	fmt.Println("1. Creating test topics...")
	for _, topic := range topics {
		result, err := admin.CreateTopic(client.CreateTopicRequest{
			Name:       topic,
			Partitions: 3,
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

	for _, topic := range topics {
		for i := 1; i <= 5; i++ {
			message := fmt.Sprintf("Message %d from %s", i, topic)
			_, err := producer.Send(client.ProduceMessage{
				Topic:     topic,
				Partition: -1,
				Value:     []byte(message),
			})
			if err != nil {
				log.Printf("Failed to send to %s: %v", topic, err)
			} else {
				fmt.Printf("  ✓ Sent to %s: %s\n", topic, message)
			}
		}
	}

	fmt.Println("\n3. Starting consumer with initial subscription...")

	// Start consumer with initial subscription
	consumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        "auto-rebalance-group",
		ConsumerID:     "consumer-1",
		Topics:         []string{"orders"}, // Start with just one topic
		SessionTimeout: 30 * time.Second,
	})

	// Join group
	fmt.Println("  Consumer joining group...")
	if err := consumer.JoinGroup(); err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}

	// Show initial assignment
	assignment := consumer.GetAssignment()
	subscription := consumer.GetSubscription()
	fmt.Printf("  ✓ Initial subscription: %v\n", subscription)
	fmt.Printf("  ✓ Initial assignment: %v\n", assignment)

	// Start consuming in background
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMessages(consumer, "Initial consumption")
	}()

	// Wait a bit for initial consumption
	time.Sleep(3 * time.Second)

	fmt.Println("\n4. 🔥 Testing automatic rebalancing on subscription change...")

	// Test 1: Subscribe to additional topic
	fmt.Println("  📝 Adding subscription to 'payments'...")
	if err := consumer.Subscribe([]string{"payments"}); err != nil {
		log.Printf("Failed to subscribe: %v", err)
	} else {
		fmt.Println("  ✅ Subscribe call completed - rebalancing should be triggered automatically!")
	}

	// Wait for rebalance to complete
	fmt.Println("  ⏳ Waiting for rebalance to complete...")
	if err := consumer.WaitForRebalanceComplete(10 * time.Second); err != nil {
		log.Printf("Rebalance timeout: %v", err)
	} else {
		fmt.Println("  ✅ Rebalance completed!")
	}

	// Show updated assignment
	time.Sleep(1 * time.Second) // Give a moment for assignment to update
	newAssignment := consumer.GetAssignment()
	newSubscription := consumer.GetSubscription()
	fmt.Printf("  📊 Updated subscription: %v\n", newSubscription)
	fmt.Printf("  📊 Updated assignment: %v\n", newAssignment)

	// Continue consuming with new assignment
	time.Sleep(3 * time.Second)

	// Test 2: Subscribe to even more topics
	fmt.Println("\n5. 🔥 Adding more topics...")
	fmt.Println("  📝 Adding subscription to 'inventory' and 'notifications'...")
	if err := consumer.Subscribe([]string{"inventory", "notifications"}); err != nil {
		log.Printf("Failed to subscribe: %v", err)
	} else {
		fmt.Println("  ✅ Subscribe call completed - another rebalancing triggered!")
	}

	// Wait for second rebalance
	fmt.Println("  ⏳ Waiting for second rebalance to complete...")
	if err := consumer.WaitForRebalanceComplete(10 * time.Second); err != nil {
		log.Printf("Rebalance timeout: %v", err)
	} else {
		fmt.Println("  ✅ Second rebalance completed!")
	}

	// Show final assignment
	time.Sleep(1 * time.Second)
	finalAssignment := consumer.GetAssignment()
	finalSubscription := consumer.GetSubscription()
	fmt.Printf("  📊 Final subscription: %v\n", finalSubscription)
	fmt.Printf("  📊 Final assignment: %v\n", finalAssignment)

	// Continue consuming from all topics
	time.Sleep(3 * time.Second)

	// Test 3: Unsubscribe from some topics
	fmt.Println("\n6. 🔥 Testing unsubscription...")
	fmt.Println("  📝 Unsubscribing from 'orders' and 'notifications'...")
	if err := consumer.Unsubscribe([]string{"orders", "notifications"}); err != nil {
		log.Printf("Failed to unsubscribe: %v", err)
	} else {
		fmt.Println("  ✅ Unsubscribe call completed - rebalancing triggered!")
	}

	// Wait for third rebalance
	fmt.Println("  ⏳ Waiting for unsubscribe rebalance to complete...")
	if err := consumer.WaitForRebalanceComplete(10 * time.Second); err != nil {
		log.Printf("Rebalance timeout: %v", err)
	} else {
		fmt.Println("  ✅ Unsubscribe rebalance completed!")
	}

	// Show final state
	time.Sleep(1 * time.Second)
	endAssignment := consumer.GetAssignment()
	endSubscription := consumer.GetSubscription()
	fmt.Printf("  📊 End subscription: %v\n", endSubscription)
	fmt.Printf("  📊 End assignment: %v\n", endAssignment)

	// Final consumption
	time.Sleep(3 * time.Second)

	// Cleanup
	consumer.LeaveGroup()
	wg.Wait()

	fmt.Println("\n✅ Automatic Rebalancing Demo Completed!")
	fmt.Println("\nKey Features Demonstrated:")
	fmt.Println("- ✅ Automatic rebalancing on Subscribe()")
	fmt.Println("- ✅ Automatic rebalancing on Unsubscribe()")
	fmt.Println("- ✅ Non-blocking rebalancing process")
	fmt.Println("- ✅ Rebalance state checking")
	fmt.Println("- ✅ Graceful handling during rebalancing")
	fmt.Println("- ✅ Dynamic partition reassignment")
}

func consumeMessages(consumer *client.GroupConsumer, phase string) {
	fmt.Printf("  🚀 Starting %s...\n", phase)
	messageCount := 0

	for messageCount < 20 { // Consume up to 20 messages
		// Check if we should stop
		if messageCount > 0 && messageCount%10 == 0 {
			fmt.Printf("  📊 %s: consumed %d messages so far\n", phase, messageCount)
		}

		messages, err := consumer.Poll(2 * time.Second)
		if err != nil {
			log.Printf("Poll error during %s: %v", phase, err)
			continue
		}

		if len(messages) == 0 {
			// No messages, check if rebalancing
			if consumer.IsRebalancing() {
				fmt.Printf("  🔄 %s: rebalancing detected, pausing consumption\n", phase)
				time.Sleep(1 * time.Second)
				continue
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, message := range messages {
			fmt.Printf("  📨 %s: consumed '%s' (topic: %s, partition: %d, offset: %d)\n",
				phase, string(message.Value), message.Topic, message.Partition, message.Offset)

			// Commit offset
			if err := consumer.CommitOffset(message.Topic, message.Partition, message.Offset, "auto"); err != nil {
				log.Printf("Failed to commit offset: %v", err)
			}

			messageCount++
			if messageCount >= 20 {
				break
			}
		}
	}

	fmt.Printf("  ✅ %s completed: consumed %d messages\n", phase, messageCount)
}
