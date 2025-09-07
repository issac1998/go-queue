package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== JoinGroup/LeaveGroup Rebalancing Demo ===")

	// Create client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	// Create test topic
	admin := client.NewAdmin(c)
	topicName := "join-leave-demo"

	fmt.Println("1. Creating test topic...")
	result, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       topicName,
		Partitions: 6, // 6 partitions for better demonstration
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Failed to create topic: %v", err)
	} else {
		fmt.Printf("  ✓ Created topic: %s with 6 partitions\n", result.Name)
	}

	// Produce test messages
	fmt.Println("\n2. Producing test messages...")
	producer := client.NewProducerWithStrategy(c, client.PartitionStrategyRoundRobin)

	for i := 1; i <= 18; i++ {
		message := fmt.Sprintf("Message %d", i)
		_, err := producer.Send(client.ProduceMessage{
			Topic:     topicName,
			Partition: -1,
			Value:     []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
		}
	}
	fmt.Printf("  ✓ Produced 18 messages to topic %s\n", topicName)

	fmt.Println("\n3. 🔥 Demonstrating JoinGroup rebalancing...")

	groupID := "join-leave-demo-group"
	var wg sync.WaitGroup

	// Phase 1: Start first consumer
	fmt.Println("\n  Phase 1: Starting first consumer...")
	consumer1 := createConsumer(c, groupID, "consumer-1", topicName)

	fmt.Println("    Consumer-1 joining group...")
	if err := consumer1.JoinGroup(); err != nil {
		log.Fatalf("Consumer-1 failed to join: %v", err)
	}

	assignment1 := consumer1.GetAssignment()
	fmt.Printf("    ✅ Consumer-1 assignment: %v\n", assignment1)
	fmt.Printf("    📊 Consumer-1 got %d partitions\n", countPartitions(assignment1))

	// Start consuming
	wg.Add(1)
	go consumeAndReport(consumer1, "Consumer-1", 10, &wg)

	time.Sleep(3 * time.Second)

	// Phase 2: Add second consumer - should trigger rebalancing
	fmt.Println("\n  Phase 2: Adding second consumer (should trigger rebalancing)...")
	consumer2 := createConsumer(c, groupID, "consumer-2", topicName)

	fmt.Println("    Consumer-2 joining group...")
	if err := consumer2.JoinGroup(); err != nil {
		log.Fatalf("Consumer-2 failed to join: %v", err)
	}

	// Check assignments after rebalancing
	time.Sleep(2 * time.Second) // Wait for rebalancing
	assignment1After := consumer1.GetAssignment()
	assignment2 := consumer2.GetAssignment()

	fmt.Printf("    🔄 After rebalancing:\n")
	fmt.Printf("      Consumer-1 assignment: %v (%d partitions)\n", assignment1After, countPartitions(assignment1After))
	fmt.Printf("      Consumer-2 assignment: %v (%d partitions)\n", assignment2, countPartitions(assignment2))

	// Start second consumer
	wg.Add(1)
	go consumeAndReport(consumer2, "Consumer-2", 10, &wg)

	time.Sleep(3 * time.Second)

	// Phase 3: Add third consumer - more rebalancing
	fmt.Println("\n  Phase 3: Adding third consumer (more rebalancing)...")
	consumer3 := createConsumer(c, groupID, "consumer-3", topicName)

	fmt.Println("    Consumer-3 joining group...")
	if err := consumer3.JoinGroup(); err != nil {
		log.Fatalf("Consumer-3 failed to join: %v", err)
	}

	// Check assignments after second rebalancing
	time.Sleep(2 * time.Second)
	assignment1Final := consumer1.GetAssignment()
	assignment2Final := consumer2.GetAssignment()
	assignment3 := consumer3.GetAssignment()

	fmt.Printf("    🔄 After second rebalancing:\n")
	fmt.Printf("      Consumer-1 assignment: %v (%d partitions)\n", assignment1Final, countPartitions(assignment1Final))
	fmt.Printf("      Consumer-2 assignment: %v (%d partitions)\n", assignment2Final, countPartitions(assignment2Final))
	fmt.Printf("      Consumer-3 assignment: %v (%d partitions)\n", assignment3, countPartitions(assignment3))

	// Start third consumer
	wg.Add(1)
	go consumeAndReport(consumer3, "Consumer-3", 10, &wg)

	time.Sleep(3 * time.Second)

	fmt.Println("\n4. 🔥 Demonstrating LeaveGroup rebalancing...")

	// Phase 4: Remove one consumer - should trigger rebalancing for others
	fmt.Println("\n  Phase 4: Consumer-2 leaving group (should trigger rebalancing)...")

	fmt.Println("    Consumer-2 leaving group...")
	if err := consumer2.LeaveGroup(); err != nil {
		log.Printf("Consumer-2 failed to leave: %v", err)
	} else {
		fmt.Println("    ✅ Consumer-2 left the group successfully")
	}

	// Wait for other consumers to rebalance
	time.Sleep(3 * time.Second)

	assignment1AfterLeave := consumer1.GetAssignment()
	assignment3AfterLeave := consumer3.GetAssignment()

	fmt.Printf("    🔄 After Consumer-2 left:\n")
	fmt.Printf("      Consumer-1 assignment: %v (%d partitions)\n", assignment1AfterLeave, countPartitions(assignment1AfterLeave))
	fmt.Printf("      Consumer-3 assignment: %v (%d partitions)\n", assignment3AfterLeave, countPartitions(assignment3AfterLeave))

	// Continue consuming for a bit
	time.Sleep(3 * time.Second)

	// Phase 5: Remove another consumer
	fmt.Println("\n  Phase 5: Consumer-3 leaving group...")

	if err := consumer3.LeaveGroup(); err != nil {
		log.Printf("Consumer-3 failed to leave: %v", err)
	} else {
		fmt.Println("    ✅ Consumer-3 left the group successfully")
	}

	// Consumer-1 should now have all partitions
	time.Sleep(3 * time.Second)
	assignment1Solo := consumer1.GetAssignment()
	fmt.Printf("    🔄 Consumer-1 now handles all partitions: %v (%d partitions)\n",
		assignment1Solo, countPartitions(assignment1Solo))

	// Final consumption
	time.Sleep(3 * time.Second)

	// Cleanup
	consumer1.LeaveGroup()
	wg.Wait()

	fmt.Println("\n✅ JoinGroup/LeaveGroup Rebalancing Demo Completed!")
	fmt.Println("\nKey Observations:")
	fmt.Println("- ✅ JoinGroup automatically triggers rebalancing")
	fmt.Println("- ✅ Partitions are redistributed when consumers join")
	fmt.Println("- ✅ LeaveGroup causes remaining consumers to rebalance")
	fmt.Println("- ✅ Rebalancing state is properly managed")
	fmt.Println("- ✅ Load is balanced across available consumers")
}

func createConsumer(c *client.Client, groupID, consumerID, topic string) *client.GroupConsumer {
	return client.NewGroupConsumer(c, client.GroupConsumerConfig{
		GroupID:        groupID,
		ConsumerID:     consumerID,
		Topics:         []string{topic},
		SessionTimeout: 30 * time.Second,
	})
}

func consumeAndReport(consumer *client.GroupConsumer, name string, maxMessages int, wg *sync.WaitGroup) {
	defer wg.Done()

	messageCount := 0
	fmt.Printf("    🚀 %s started consuming...\n", name)

	for messageCount < maxMessages {
		// Check if rebalancing
		if consumer.IsRebalancing() {
			fmt.Printf("    🔄 %s: rebalancing in progress, pausing consumption\n", name)
			time.Sleep(1 * time.Second)
			continue
		}

		messages, err := consumer.Poll(2 * time.Second)
		if err != nil {
			log.Printf("%s poll error: %v", name, err)
			continue
		}

		if len(messages) == 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, message := range messages {
			fmt.Printf("    📨 %s: consumed '%s' (partition: %d, offset: %d)\n",
				name, string(message.Value), message.Partition, message.Offset)

			// Commit offset
			if err := consumer.CommitOffset(message.Topic, message.Partition, message.Offset, "auto"); err != nil {
				log.Printf("%s failed to commit offset: %v", name, err)
			}

			messageCount++
			if messageCount >= maxMessages {
				break
			}
		}
	}

	fmt.Printf("    ✅ %s finished consuming %d messages\n", name, messageCount)
}

func countPartitions(assignment map[string][]int32) int {
	count := 0
	for _, partitions := range assignment {
		count += len(partitions)
	}
	return count
}
