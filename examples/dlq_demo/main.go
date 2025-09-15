package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/dlq"
)

func main() {
	// Create client configuration
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}

	// Create client
	client := client.NewClient(clientConfig)
	defer client.Close()

	// Create DLQ configuration
	dlqConfig := dlq.DefaultDLQConfig()
	dlqConfig.RetryPolicy.MaxRetries = 3
	dlqConfig.RetryPolicy.InitialDelay = 2 * time.Second
	dlqConfig.RetryPolicy.BackoffFactor = 2.0
	dlqConfig.RetryPolicy.MaxDelay = 30 * time.Second
	dlqConfig.TopicSuffix = ".dlq"

	// Create DLQ manager
	dlqManager, err := dlq.NewManager(client, dlqConfig)
	if err != nil {
		log.Fatalf("Failed to create DLQ manager: %v", err)
	}

	fmt.Println("DLQ Example Started")
	fmt.Println("==================")

	// Example 1: Basic DLQ configuration
	runConfigurationExample(dlqManager)

	// Example 2: Message failure handling
	runFailureHandlingExample(dlqManager)

	// Example 3: DLQ monitoring
	runMonitoringExample(dlqManager)

	// Example 4: Retry logic demonstration
	runRetryLogicExample(dlqManager)

	fmt.Println("\nDLQ Example Completed")
}

func runConfigurationExample(dlqManager *dlq.Manager) {
	fmt.Println("\n1. DLQ Configuration Example")
	fmt.Println("-----------------------------")

	// Show current configuration
	fmt.Println("DLQ Manager created successfully with configuration:")
	fmt.Println("- Enabled: true")
	fmt.Println("- Topic Suffix: .dlq")
	fmt.Println("- Max Retries: 3")
	fmt.Println("- Initial Delay: 2s")
	fmt.Println("- Backoff Factor: 2.0")
	fmt.Println("- Max Delay: 30s")

	fmt.Println("Configuration example completed")
}

func runFailureHandlingExample(dlqManager *dlq.Manager) {
	fmt.Println("\n2. Message Failure Handling Example")
	fmt.Println("------------------------------------")

	// Simulate a failed message
	message := &client.Message{
		Topic:     "orders",
		Partition: 0,
		Offset:    100,
		Value:     []byte("Order #1: Product ABC, Quantity: 10"),
	}

	failureInfo := &dlq.FailureInfo{
		ConsumerGroup: "order-processor",
		ConsumerID:    "consumer-1",
		ErrorMessage:  "Database connection timeout",
		FailureTime:   time.Now(),
	}

	fmt.Printf("Handling failed message: %s\n", string(message.Value))
	fmt.Printf("Failure reason: %s\n", failureInfo.ErrorMessage)

	err := dlqManager.HandleFailedMessage(message, failureInfo)
	if err != nil {
		fmt.Printf("Failed to handle failed message: %v\n", err)
	} else {
		fmt.Println("Message failure handled successfully")
	}

	fmt.Println("Failure handling example completed")
}

func runMonitoringExample(dlqManager *dlq.Manager) {
	fmt.Println("\n3. DLQ Monitoring Example")
	fmt.Println("--------------------------")

	// Get DLQ statistics
	stats := dlqManager.GetDLQStats()
	fmt.Printf("DLQ Statistics:\n")
	fmt.Printf("  Total Messages: %d\n", stats.TotalMessages)
	fmt.Printf("  Messages Last Hour: %d\n", stats.MessagesLastHour)
	fmt.Printf("  Messages Last Day: %d\n", stats.MessagesLastDay)

	if len(stats.TopFailureReasons) > 0 {
		fmt.Printf("  Top Failure Reasons:\n")
		for i, reason := range stats.TopFailureReasons {
			fmt.Printf("    %d. %s\n", i+1, reason)
		}
	} else {
		fmt.Printf("  No failure reasons recorded yet\n")
	}

	if !stats.OldestMessage.IsZero() {
		fmt.Printf("  Oldest Message: %v\n", stats.OldestMessage.Format(time.RFC3339))
	}
	if !stats.NewestMessage.IsZero() {
		fmt.Printf("  Newest Message: %v\n", stats.NewestMessage.Format(time.RFC3339))
	}

	fmt.Println("Monitoring example completed")
}

func runRetryLogicExample(dlqManager *dlq.Manager) {
	fmt.Println("\n4. Retry Logic Example")
	fmt.Println("-----------------------")

	// Check retry status for specific messages
	topic := "orders"
	partition := int32(0)
	offset := int64(100)
	consumerGroup := "order-processor"

	fmt.Printf("Checking retry status for message:\n")
	fmt.Printf("  Topic: %s\n", topic)
	fmt.Printf("  Partition: %d\n", partition)
	fmt.Printf("  Offset: %d\n", offset)
	fmt.Printf("  Consumer Group: %s\n", consumerGroup)

	shouldRetry := dlqManager.ShouldRetryMessage(topic, partition, offset, consumerGroup)
	fmt.Printf("\nShould retry: %v\n", shouldRetry)

	if shouldRetry {
		retryDelay := dlqManager.GetRetryDelay(topic, partition, offset, consumerGroup)
		fmt.Printf("Retry delay: %v\n", retryDelay)
		fmt.Printf("Next retry time: %v\n", time.Now().Add(retryDelay).Format(time.RFC3339))
	} else {
		fmt.Println("Message does not need retry or has exceeded max retries")
	}

	// Demonstrate retry policy calculation
	fmt.Println("\nRetry delay calculation for different attempt numbers:")
	for attempt := 1; attempt <= 5; attempt++ {
		delay := calculateRetryDelay(attempt, dlqManager)
		fmt.Printf("  Attempt %d: %v\n", attempt, delay)
	}

	fmt.Println("Retry logic example completed")
}

// Helper function to demonstrate retry delay calculation
func calculateRetryDelay(attemptNumber int, dlqManager *dlq.Manager) time.Duration {
	// This is a simplified version of the retry delay calculation
	// In practice, this would be handled internally by the DLQ manager
	initialDelay := 2 * time.Second
	backoffFactor := 2.0
	maxDelay := 30 * time.Second

	delay := initialDelay
	for i := 1; i < attemptNumber; i++ {
		delay = time.Duration(float64(delay) * backoffFactor)
		if delay > maxDelay {
			delay = maxDelay
			break
		}
	}

	return delay
}