package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Consumer Groups Real Consumption Demo ===")

	// Create a client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	fmt.Println("\n1. Creating test topic with 4 partitions...")
	admin := client.NewAdmin(c)
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "consumption-demo",
		Partitions: 4,
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
	} else if createResult.Error != nil {
		log.Printf("Topic creation error: %v", createResult.Error)
	} else {
		fmt.Printf("✓ Topic '%s' created successfully with 4 partitions!\n", createResult.Name)
	}

	fmt.Println("\n2. Producing 20 test messages...")
	producer := client.NewProducerWithStrategy(c, client.PartitionStrategyRoundRobin)

	for i := 0; i < 20; i++ {
		message := fmt.Sprintf("Message %d - Hello from producer!", i+1)
		result, err := producer.Send(client.ProduceMessage{
			Topic:     "consumption-demo",
			Partition: -1, // Auto-assign partition
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

		fmt.Printf("  ✓ Sent message %d to partition %d (offset: %d)\n", i+1, result.Partition, result.Offset)
	}

	fmt.Println("\n3. Starting 3 consumers in the same group...")

	groupID := "demo-consumption-group"
	topic := "consumption-demo"

	var wg sync.WaitGroup
	consumers := make([]*client.GroupConsumer, 3)

	// Message tracking
	consumedMessages := make(map[string][]string) // consumerID -> messages
	var mu sync.Mutex

	// Start 3 consumers
	for i := 0; i < 3; i++ {
		consumerID := fmt.Sprintf("consumer-%d", i+1)

		consumers[i] = client.NewGroupConsumer(c, client.GroupConsumerConfig{
			GroupID:        groupID,
			ConsumerID:     consumerID,
			Topics:         []string{topic},
			SessionTimeout: 30 * time.Second,
		})

		// Join the group
		fmt.Printf("  %s joining group...\n", consumerID)
		if err := consumers[i].JoinGroup(); err != nil {
			log.Printf("Consumer %s failed to join group: %v", consumerID, err)
			continue
		}

		assignment := consumers[i].GetAssignment()
		fmt.Printf("  ✓ %s joined! Assignment: %v\n", consumerID, assignment)

		// Start consuming in goroutine
		wg.Add(1)
		go func(consumer *client.GroupConsumer, id string) {
			defer wg.Done()
			defer consumer.LeaveGroup()

			fmt.Printf("  🚀 %s starting consumption...\n", id)

			messageCount := 0
			consumedByThisConsumer := make([]string, 0)

			// Method 1: Using Poll (manual control)
			for messageCount < 10 { // Each consumer will consume up to 10 messages
				messages, err := consumer.Poll(2 * time.Second)
				if err != nil {
					log.Printf("%s poll error: %v", id, err)
					continue
				}

				for _, message := range messages {
					messageStr := string(message.Value)
					consumedByThisConsumer = append(consumedByThisConsumer, messageStr)

					fmt.Printf("  📨 %s consumed: '%s' (topic: %s, partition: %d, offset: %d)\n",
						id, messageStr, message.Topic, message.Partition, message.Offset)

					// Commit offset
					err := consumer.CommitOffset(message.Topic, message.Partition, message.Offset, "processed")
					if err != nil {
						log.Printf("%s failed to commit offset: %v", id, err)
					}

					messageCount++
					if messageCount >= 10 {
						break
					}
				}

				if len(messages) == 0 {
					time.Sleep(500 * time.Millisecond)
				}
			}

			mu.Lock()
			consumedMessages[id] = consumedByThisConsumer
			mu.Unlock()

			fmt.Printf("  ✅ %s finished consuming %d messages\n", id, messageCount)
		}(consumers[i], consumerID)
	}

	// Wait for all consumers to finish
	fmt.Println("\n4. Waiting for consumers to finish...")
	wg.Wait()

	fmt.Println("\n5. Consumption Summary:")
	mu.Lock()
	totalConsumed := 0
	for consumerID, messages := range consumedMessages {
		fmt.Printf("  %s consumed %d messages:\n", consumerID, len(messages))
		for i, msg := range messages {
			if i < 3 { // Show first 3 messages
				fmt.Printf("    - %s\n", msg)
			} else if i == 3 {
				fmt.Printf("    - ... and %d more\n", len(messages)-3)
				break
			}
		}
		totalConsumed += len(messages)
	}
	mu.Unlock()

	fmt.Printf("\n📊 Total messages consumed: %d\n", totalConsumed)

	fmt.Println("\n6. Testing consumer group with auto-consumption...")

	// Create another consumer for auto-consumption demo
	autoConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        "auto-consumption-group",
		ConsumerID:     "auto-consumer",
		Topics:         []string{topic},
		SessionTimeout: 30 * time.Second,
	})

	fmt.Println("  Auto-consumer joining group...")
	if err := autoConsumer.JoinGroup(); err != nil {
		log.Printf("Auto-consumer failed to join group: %v", err)
	} else {
		fmt.Println("  ✓ Auto-consumer joined successfully!")

		// Start auto-consumption in background
		go func() {
			defer autoConsumer.LeaveGroup()

			// Method 2: Using Consume (automatic handling)
			err := autoConsumer.Consume(func(message *client.ConsumeMessage) error {
				fmt.Printf("  🔄 Auto-consumed: '%s' (partition: %d, offset: %d)\n",
					string(message.Value), message.Partition, message.Offset)
				return nil
			})
			if err != nil {
				log.Printf("Auto-consumption error: %v", err)
			}
		}()

		// Let it run for a few seconds
		time.Sleep(5 * time.Second)
		fmt.Println("  ✅ Auto-consumption demo completed")
	}

	fmt.Println("\n✅ Consumer Groups Real Consumption Demo Completed!")
	fmt.Println("\nKey Features Demonstrated:")
	fmt.Println("- Multiple consumers in the same group")
	fmt.Println("- Automatic partition assignment")
	fmt.Println("- Message consumption from assigned partitions")
	fmt.Println("- Offset management and committing")
	fmt.Println("- Both manual (Poll) and automatic (Consume) consumption")
	fmt.Println("- Consumer group coordination and load balancing")
}
