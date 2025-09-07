package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("🚀 Optimized Consumer Group Rebalance Demo")
	fmt.Println("This demo showcases the improved rebalance mechanism where consumers")
	fmt.Println("detect generation changes through heartbeat and fetch new assignments")
	fmt.Println("directly instead of leaving and rejoining the group.")
	fmt.Println()

	// Create client
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	}

	clientInstance := client.NewClient(clientConfig)

	// Create topic for testing
	admin := client.NewAdmin(clientInstance)
	_, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "optimized-rebalance-topic",
		Partitions: 6,
		Replicas:   1,
	})
	if err != nil {
		log.Printf("Failed to create topic (may already exist): %v", err)
	}

	fmt.Println("1. 🌟 Creating first consumer...")

	// Create first consumer
	consumer1 := client.NewGroupConsumer(clientInstance, client.GroupConsumerConfig{
		GroupID:    "optimized-group",
		ConsumerID: "optimized-consumer-1",
		Topics:     []string{"optimized-rebalance-topic"},
	})

	if err := consumer1.JoinGroup(); err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}

	assignment1 := consumer1.GetAssignment()
	fmt.Printf("   Consumer-1 initial assignment: %v (%d partitions)\n",
		assignment1, countPartitions(assignment1))

	time.Sleep(2 * time.Second)

	fmt.Println("\n2. 🔄 Adding second consumer (triggers rebalance)...")

	// Create second consumer
	consumer2 := client.NewGroupConsumer(clientInstance, client.GroupConsumerConfig{
		GroupID:    "optimized-group",
		ConsumerID: "optimized-consumer-2",
		Topics:     []string{"optimized-rebalance-topic"},
	})

	if err := consumer2.JoinGroup(); err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}

	// Wait for first consumer to detect the change via heartbeat
	fmt.Println("   Waiting for Consumer-1 to detect rebalance via heartbeat...")
	time.Sleep(3 * time.Second)

	assignment1After := consumer1.GetAssignment()
	assignment2 := consumer2.GetAssignment()

	fmt.Printf("   🔥 After optimized rebalance:\n")
	fmt.Printf("      Consumer-1 assignment: %v (%d partitions)\n",
		assignment1After, countPartitions(assignment1After))
	fmt.Printf("      Consumer-2 assignment: %v (%d partitions)\n",
		assignment2, countPartitions(assignment2))

	time.Sleep(2 * time.Second)

	fmt.Println("\n3. 📊 Adding third consumer...")

	// Create third consumer
	consumer3 := client.NewGroupConsumer(clientInstance, client.GroupConsumerConfig{
		GroupID:    "optimized-group",
		ConsumerID: "optimized-consumer-3",
		Topics:     []string{"optimized-rebalance-topic"},
	})

	if err := consumer3.JoinGroup(); err != nil {
		log.Fatalf("Failed to join group: %v", err)
	}

	// Wait for other consumers to detect the change
	fmt.Println("   Waiting for existing consumers to detect rebalance...")
	time.Sleep(3 * time.Second)

	assignment1Final := consumer1.GetAssignment()
	assignment2Final := consumer2.GetAssignment()
	assignment3 := consumer3.GetAssignment()

	fmt.Printf("   🔥 After second optimized rebalance:\n")
	fmt.Printf("      Consumer-1 assignment: %v (%d partitions)\n",
		assignment1Final, countPartitions(assignment1Final))
	fmt.Printf("      Consumer-2 assignment: %v (%d partitions)\n",
		assignment2Final, countPartitions(assignment2Final))
	fmt.Printf("      Consumer-3 assignment: %v (%d partitions)\n",
		assignment3, countPartitions(assignment3))

	time.Sleep(2 * time.Second)

	fmt.Println("\n4. ⚡ Demonstrating optimized consumer departure...")

	// Remove consumer 2
	fmt.Println("   Consumer-2 leaving group...")
	if err := consumer2.LeaveGroup(); err != nil {
		log.Printf("Consumer-2 failed to leave: %v", err)
	} else {
		fmt.Println("   ✅ Consumer-2 left successfully")
	}

	// Wait for others to rebalance
	fmt.Println("   Waiting for remaining consumers to rebalance...")
	time.Sleep(3 * time.Second)

	assignment1End := consumer1.GetAssignment()
	assignment3End := consumer3.GetAssignment()

	fmt.Printf("   🔥 After Consumer-2 departure:\n")
	fmt.Printf("      Consumer-1 assignment: %v (%d partitions)\n",
		assignment1End, countPartitions(assignment1End))
	fmt.Printf("      Consumer-3 assignment: %v (%d partitions)\n",
		assignment3End, countPartitions(assignment3End))

	// Cleanup
	consumer1.LeaveGroup()
	consumer3.LeaveGroup()

	fmt.Println("\n✅ Optimized Rebalance Demo Completed!")
	fmt.Println("\n🎯 Key Improvements:")
	fmt.Println("- ⚡ Consumers detect generation changes via heartbeat")
	fmt.Println("- 🚀 Direct assignment fetching instead of leave/rejoin")
	fmt.Println("- ⏱️  Faster rebalancing with less network overhead")
	fmt.Println("- 🔄 Reduced disruption to message consumption")
}

func countPartitions(assignment map[string][]int32) int {
	count := 0
	for _, partitions := range assignment {
		count += len(partitions)
	}
	return count
}
