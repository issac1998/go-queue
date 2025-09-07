package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Partitioning Strategies Demo ===")

	// Create a client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	fmt.Println("\n1. Creating test topic with 4 partitions...")
	admin := client.NewAdmin(c)
	createResult, err := admin.CreateTopic(client.CreateTopicRequest{
		Name:       "partitioning-demo",
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

	// Demo 1: Manual Partitioning (Default)
	fmt.Println("\n2. Manual Partitioning (Default Strategy):")
	manualProducer := client.NewProducer(c)

	for i := 0; i < 4; i++ {
		message := fmt.Sprintf("Manual message %d", i+1)
		result, err := manualProducer.Send(client.ProduceMessage{
			Topic:     "partitioning-demo",
			Partition: int32(i % 4), // Manually specify partition
			Value:     []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		fmt.Printf("  ✓ '%s' -> Partition %d (Offset: %d)\n", message, result.Partition, result.Offset)
	}

	// Demo 2: Round-Robin Partitioning
	fmt.Println("\n3. Round-Robin Partitioning:")
	rrProducer := client.NewProducerWithStrategy(c, client.PartitionStrategyRoundRobin)

	for i := 0; i < 6; i++ {
		message := fmt.Sprintf("RoundRobin message %d", i+1)
		result, err := rrProducer.Send(client.ProduceMessage{
			Topic:     "partitioning-demo",
			Partition: -1, // Let partitioner decide
			Value:     []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		fmt.Printf("  ✓ '%s' -> Partition %d (Offset: %d)\n", message, result.Partition, result.Offset)
	}

	// Demo 3: Random Partitioning
	fmt.Println("\n4. Random Partitioning:")
	randomProducer := client.NewProducerWithStrategy(c, client.PartitionStrategyRandom)

	for i := 0; i < 6; i++ {
		message := fmt.Sprintf("Random message %d", i+1)
		result, err := randomProducer.Send(client.ProduceMessage{
			Topic:     "partitioning-demo",
			Partition: -1, // Let partitioner decide
			Value:     []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		fmt.Printf("  ✓ '%s' -> Partition %d (Offset: %d)\n", message, result.Partition, result.Offset)
	}

	// Demo 4: Hash-based Partitioning
	fmt.Println("\n5. Hash-based Partitioning (with keys):")
	hashProducer := client.NewProducerWithStrategy(c, client.PartitionStrategyHash)

	keys := []string{"user-1", "user-2", "user-3", "user-1", "user-2", "user-4"}
	for i, key := range keys {
		message := fmt.Sprintf("Hash message %d", i+1)
		result, err := hashProducer.Send(client.ProduceMessage{
			Topic:     "partitioning-demo",
			Partition: -1, // Let partitioner decide
			Key:       []byte(key),
			Value:     []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		fmt.Printf("  ✓ '%s' (key: %s) -> Partition %d (Offset: %d)\n", message, key, result.Partition, result.Offset)
	}

	// Demo 5: Hash-based Partitioning without keys (falls back to random)
	fmt.Println("\n6. Hash-based Partitioning (without keys - random fallback):")

	for i := 0; i < 4; i++ {
		message := fmt.Sprintf("Hash no-key message %d", i+1)
		result, err := hashProducer.Send(client.ProduceMessage{
			Topic:     "partitioning-demo",
			Partition: -1, // Let partitioner decide
			// No Key provided - will use random
			Value: []byte(message),
		})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
			continue
		}
		fmt.Printf("  ✓ '%s' (no key) -> Partition %d (Offset: %d)\n", message, result.Partition, result.Offset)
	}

	fmt.Println("\n✓ Partitioning strategies demo completed!")
	fmt.Println("\nKey observations:")
	fmt.Println("- Manual: You control exact partition placement")
	fmt.Println("- Round-Robin: Even distribution across partitions")
	fmt.Println("- Random: Random distribution for load balancing")
	fmt.Println("- Hash: Same key always goes to same partition (consistency)")
}
