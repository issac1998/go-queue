package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	log.Println("=== Ordered Producer Async I/O Demo ===")

	// Create client with async I/O enabled
	clientConfig := client.ClientConfig{
		BrokerAddrs:   []string{"localhost:9092", "localhost:9093"},
		Timeout:       10 * time.Second,
		EnableAsyncIO: true,
	}

	c := client.NewClient(clientConfig)
	orderedProducer := client.NewOrderedProducer(c)

	topic := "ordered-async-test"

	// Test 1: Single message group with async I/O
	log.Println("\n--- Test 1: Single Message Group Async ---")
	testSingleMessageGroupAsync(orderedProducer, topic)

	// Test 2: Multiple message groups with async I/O
	log.Println("\n--- Test 2: Multiple Message Groups Async ---")
	testMultipleMessageGroupsAsync(orderedProducer, topic)

	// Test 3: Performance comparison
	log.Println("\n--- Test 3: Performance Comparison ---")
	testPerformanceComparison(orderedProducer, topic)

	log.Println("\n=== Demo completed ===")
}

func testSingleMessageGroupAsync(producer *client.OrderedProducer, topic string) {
	messageGroup := "user-123"
	messages := []client.OrderedMessage{
		{Value: []byte("Order created"), MessageGroup: messageGroup},
		{Value: []byte("Payment processed"), MessageGroup: messageGroup},
		{Value: []byte("Order shipped"), MessageGroup: messageGroup},
		{Value: []byte("Order delivered"), MessageGroup: messageGroup},
	}

	start := time.Now()
	result, err := producer.SendOrderedMessages(topic, messages)
	if err != nil {
		log.Printf("Error sending ordered messages: %v", err)
		return
	}

	duration := time.Since(start)
	log.Printf("Sent %d messages for group '%s' in %v", len(messages), messageGroup, duration)
	log.Printf("Success rate: %d/%d messages", result.SuccessfulMessages, result.TotalMessages)

	// Show message group routing
	for group, partition := range result.MessageGroupRouting {
		log.Printf("Message group '%s' routed to partition %d", group, partition)
	}
}

func testMultipleMessageGroupsAsync(producer *client.OrderedProducer, topic string) {
	messageGroups := []string{"user-456", "user-789", "user-101"}
	allMessages := make([]client.OrderedMessage, 0)

	// Create messages for different groups
	for _, group := range messageGroups {
		groupMessages := []client.OrderedMessage{
			{Value: []byte(fmt.Sprintf("%s: Action 1", group)), MessageGroup: group},
			{Value: []byte(fmt.Sprintf("%s: Action 2", group)), MessageGroup: group},
			{Value: []byte(fmt.Sprintf("%s: Action 3", group)), MessageGroup: group},
		}
		allMessages = append(allMessages, groupMessages...)
	}

	start := time.Now()
	result, err := producer.SendOrderedMessages(topic, allMessages)
	if err != nil {
		log.Printf("Error sending multiple group messages: %v", err)
		return
	}

	duration := time.Since(start)
	log.Printf("Sent %d messages for %d groups in %v", len(allMessages), len(messageGroups), duration)
	log.Printf("Success rate: %d/%d messages", result.SuccessfulMessages, result.TotalMessages)

	// Show routing for each group
	for group, partition := range result.MessageGroupRouting {
		log.Printf("Message group '%s' routed to partition %d", group, partition)
	}
}

func testPerformanceComparison(producer *client.OrderedProducer, topic string) {
	messageCount := 50
	messageGroup := "perf-test"

	// Prepare messages
	messages := make([]client.OrderedMessage, messageCount)
	for i := 0; i < messageCount; i++ {
		messages[i] = client.OrderedMessage{
			Value:        []byte(fmt.Sprintf("Performance test message %d", i+1)),
			MessageGroup: messageGroup,
		}
	}

	// Test with async I/O enabled
	log.Printf("Testing with async I/O enabled...")
	start := time.Now()
	result, err := producer.SendOrderedMessages(topic, messages)
	if err != nil {
		log.Printf("Async test error: %v", err)
		return
	}
	asyncDuration := time.Since(start)

	log.Printf("Async I/O Results:")
	log.Printf("  - Duration: %v", asyncDuration)
	log.Printf("  - Average per message: %v", asyncDuration/time.Duration(messageCount))
	log.Printf("  - Success rate: %d/%d", result.SuccessfulMessages, result.TotalMessages)
	log.Printf("  - Throughput: %.2f messages/second", float64(messageCount)/asyncDuration.Seconds())

	// Create a new producer with async I/O disabled for comparison
	syncConfig := client.ClientConfig{
		BrokerAddrs:   []string{"localhost:9092", "localhost:9093"},
		Timeout:       10 * time.Second,
		EnableAsyncIO: false,
	}
	syncClient := client.NewClient(syncConfig)
	syncProducer := client.NewOrderedProducer(syncClient)

	// Test with sync I/O
	log.Printf("\nTesting with sync I/O...")
	start = time.Now()
	syncResult, err := syncProducer.SendOrderedMessages(topic, messages)
	if err != nil {
		log.Printf("Sync test error: %v", err)
		return
	}
	syncDuration := time.Since(start)

	log.Printf("Sync I/O Results:")
	log.Printf("  - Duration: %v", syncDuration)
	log.Printf("  - Average per message: %v", syncDuration/time.Duration(messageCount))
	log.Printf("  - Success rate: %d/%d", syncResult.SuccessfulMessages, syncResult.TotalMessages)
	log.Printf("  - Throughput: %.2f messages/second", float64(messageCount)/syncDuration.Seconds())

	// Performance comparison
	if syncDuration > 0 {
		speedup := float64(syncDuration) / float64(asyncDuration)
		percentImprovement := (float64(syncDuration-asyncDuration) / float64(syncDuration)) * 100
		log.Printf("\nPerformance Comparison:")
		log.Printf("  - Async is %.2fx faster than sync", speedup)
		log.Printf("  - Performance improvement: %.1f%%", percentImprovement)
	}

	log.Printf("\nKey Benefits of Ordered Producer Async I/O:")
	log.Printf("  ✅ Maintains message ordering within each message group")
	log.Printf("  ✅ Allows concurrent processing of different message groups")
	log.Printf("  ✅ Reduces latency through non-blocking I/O operations")
	log.Printf("  ✅ Automatic fallback to sync I/O on errors")
	log.Printf("  ✅ Same API as sync version - no code changes needed")
}