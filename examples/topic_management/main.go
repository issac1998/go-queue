package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	fmt.Println("=== Go Queue Topic Management Demo ===")

	// Create a client
	c := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	})

	admin := client.NewAdmin(c)
	producer := client.NewProducer(c)

	fmt.Println("\n1. Creating test topics...")

	testTopics := []struct {
		name       string
		partitions int32
	}{
		{"topic-management-test-1", 2},
		{"topic-management-test-2", 4},
		{"topic-management-test-3", 1},
	}

	for _, topic := range testTopics {
		result, err := admin.CreateTopic(client.CreateTopicRequest{
			Name:       topic.name,
			Partitions: topic.partitions,
			Replicas:   1,
		})
		if err != nil {
			log.Printf("Failed to create topic %s: %v", topic.name, err)
		} else if result.Error != nil {
			log.Printf("Error creating topic %s: %v", topic.name, result.Error)
		} else {
			fmt.Printf("  ✓ Created topic '%s' with %d partitions\n", topic.name, topic.partitions)
		}
	}

	fmt.Println("\n2. Sending test messages...")
	for i, topic := range testTopics {
		for j := 0; j < 5; j++ {
			message := fmt.Sprintf("Test message %d for %s", j+1, topic.name)
			_, err := producer.Send(client.ProduceMessage{
				Topic:     topic.name,
				Partition: int32(j % int(topic.partitions)),
				Value:     []byte(message),
			})
			if err != nil {
				log.Printf("Failed to send message to %s: %v", topic.name, err)
			}
		}
		fmt.Printf("  ✓ Sent 5 messages to topic '%s'\n", topic.name)
		if i < len(testTopics)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// 3. List all topics
	fmt.Println("\n3. Listing all topics...")
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Failed to list topics: %v", err)
	} else {
		fmt.Printf("Found %d topics:\n", len(topics))
		for _, topic := range topics {
			fmt.Printf("  - %s: %d partitions, %d messages, %.2f KB\n",
				topic.Name, topic.Partitions, topic.MessageCount, float64(topic.Size)/1024)
		}
	}

	// 4. Describe topic details
	fmt.Println("\n4. Describing topic details...")
	topicToDescribe := testTopics[1].name
	topicDetail, err := admin.DescribeTopic(topicToDescribe)
	if err != nil {
		log.Printf("Failed to describe topic %s: %v", topicToDescribe, err)
	} else {
		fmt.Printf("Topic '%s' details:\n", topicDetail.Name)
		fmt.Printf("  Partitions: %d\n", topicDetail.Partitions)
		fmt.Printf("  Replicas: %d\n", topicDetail.Replicas)
		fmt.Printf("  Total Size: %.2f KB\n", float64(topicDetail.Size)/1024)
		fmt.Printf("  Total Messages: %d\n", topicDetail.MessageCount)
		fmt.Printf("  Created At: %s\n", topicDetail.CreatedAt.Format(time.RFC3339))

		fmt.Printf("  Partition Details:\n")
		for _, partition := range topicDetail.PartitionDetails {
			fmt.Printf("    Partition %d: Leader=%d, Messages=%d, Size=%.2f KB, Offsets=%d-%d\n",
				partition.ID, partition.Leader, partition.MessageCount,
				float64(partition.Size)/1024, partition.StartOffset, partition.EndOffset)
		}
	}

	// 5. Get simple topic info
	fmt.Println("\n5. Getting simple topic info...")
	simpleInfo, err := admin.GetTopicInfo(testTopics[0].name)
	if err != nil {
		log.Printf("Failed to get topic info: %v", err)
	} else {
		fmt.Printf("Simple info for '%s':\n", simpleInfo.Name)
		fmt.Printf("  Partitions: %d, Messages: %d, Size: %.2f KB\n",
			simpleInfo.Partitions, simpleInfo.MessageCount, float64(simpleInfo.Size)/1024)
	}

	// 6. Delete a topic
	fmt.Println("\n6. Deleting a topic...")
	topicToDelete := testTopics[2].name
	fmt.Printf("Deleting topic '%s'...\n", topicToDelete)
	err = admin.DeleteTopic(topicToDelete)
	if err != nil {
		log.Printf("Failed to delete topic %s: %v", topicToDelete, err)
	} else {
		fmt.Printf("  ✓ Topic '%s' deleted successfully\n", topicToDelete)
	}

	// 7. Verify deletion
	fmt.Println("\n7. Verifying deletion by listing topics again...")
	topicsAfterDeletion, err := admin.ListTopics()
	if err != nil {
		log.Printf("Failed to list topics after deletion: %v", err)
	} else {
		fmt.Printf("Topics after deletion (%d remaining):\n", len(topicsAfterDeletion))
		for _, topic := range topicsAfterDeletion {
			fmt.Printf("  - %s: %d partitions, %d messages\n",
				topic.Name, topic.Partitions, topic.MessageCount)
		}
	}

	// 8. Cleanup test topics
	fmt.Println("\n8. Cleaning up remaining test topics...")
	for _, topic := range testTopics[:2] {
		fmt.Printf("Deleting topic '%s'...\n", topic.name)
		err := admin.DeleteTopic(topic.name)
		if err != nil {
			log.Printf("Failed to delete topic %s: %v", topic.name, err)
		} else {
			fmt.Printf("  ✓ Topic '%s' deleted\n", topic.name)
		}
	}

	fmt.Println("\n=== Topic Management Demo Completed! ===")
	fmt.Println("\n✅ Demonstrated features:")
	fmt.Println("  - Topic creation with different partition counts")
	fmt.Println("  - Message production to test topics")
	fmt.Println("  - Listing all topics with summary information")
	fmt.Println("  - Describing detailed topic information including partition details")
	fmt.Println("  - Getting simple topic information")
	fmt.Println("  - Topic deletion")
	fmt.Println("  - Verification of operations")
}
