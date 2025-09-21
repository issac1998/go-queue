package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/dlq"
)

// Message alias for client.Message
type Message = client.Message

func main() {
	fmt.Println("DLQ Consumer Demo")
	fmt.Println("=================")

	// Create client
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     10 * time.Second,
	}
	client := client.NewClient(clientConfig)
	defer client.Close()

	// Create DLQ configuration
	dlqConfig := dlq.DefaultDLQConfig()
	dlqConfig.RetryPolicy.MaxRetries = 3
	dlqConfig.RetryPolicy.InitialDelay = 1 * time.Second
	dlqConfig.RetryPolicy.BackoffFactor = 2.0
	dlqConfig.RetryPolicy.MaxDelay = 10 * time.Second
	dlqConfig.TopicSuffix = ".dlq"

	// Demo 1: Basic DLQ consumer usage
	runBasicConsumerExample(client, dlqConfig)

	// Demo 2: DLQ consumer with custom configuration
	runCustomProcessorExample(client, dlqConfig)

	// Demo 3: DLQ consumer with failure simulation
	runFailureSimulationExample(client, dlqConfig)

	// Demo 4: ProcessMessage method usage
	processMessageDemo(client, dlqConfig)

	fmt.Println("\nAll DLQ Consumer demos completed!")
}

func runBasicConsumerExample(client *client.Client, config *dlq.DLQConfig) {
	fmt.Println("\n1. Basic DLQ Consumer Example")
	fmt.Println("------------------------------")

	// Create DLQ-enabled consumer
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "order-processor", "consumer-1", config)
	if err != nil {
		log.Printf("Failed to create DLQ consumer: %v", err)
		return
	}
	defer consumer.Stop()

	// Simulate processing a message
	message := &Message{
		Topic:     "orders",
		Partition: 0,
		Offset:    100,
		Value:     []byte("Order #1: Product ABC, Quantity: 10"),
	}

	// Define a simple processor that always succeeds
	processor := func(msg *Message) error {
		fmt.Printf("Processing message: %s\n", string(msg.Value))
		fmt.Printf("Message processed successfully\n")
		return nil
	}

	// Process the message using DLQ consumer
	err = consumer.ProcessMessage(message, processor)
	if err != nil {
		fmt.Printf("Failed to process message: %v\n", err)
	} else {
		fmt.Println("Message processed with DLQ handling")
	}

	fmt.Println("Basic consumer example completed")
}

func runCustomProcessorExample(client *client.Client, config *dlq.DLQConfig) {
	fmt.Println("\n2. Custom Processor Example")
	fmt.Println("----------------------------")

	// Create DLQ-enabled consumer
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "payment-processor", "consumer-2", config)
	if err != nil {
		log.Printf("Failed to create DLQ consumer: %v", err)
		return
	}
	defer consumer.Stop()

	// Simulate processing a payment message
	message := &Message{
		Topic:     "payments",
		Partition: 1,
		Offset:    200,
		Value:     []byte(`{"payment_id": "pay_123", "amount": 99.99, "currency": "USD"}`),
	}

	// Define a custom processor with business logic
	processor := func(msg *Message) error {
		fmt.Printf("Processing payment: %s\n", string(msg.Value))
		
		// Simulate payment processing logic
		time.Sleep(100 * time.Millisecond)
		
		// Simulate successful payment processing
		fmt.Println("Payment processed successfully")
		fmt.Println("Payment confirmation sent")
		
		return nil
	}

	// Process the message
	err = consumer.ProcessMessage(message, processor)
	if err != nil {
		fmt.Printf("Failed to process payment: %v\n", err)
	} else {
		fmt.Println("Payment processed with DLQ handling")
	}

	fmt.Println("Custom processor example completed")
}

func runFailureSimulationExample(client *client.Client, config *dlq.DLQConfig) {
	fmt.Println("\n3. Failure Simulation Example")
	fmt.Println("------------------------------")

	// Create DLQ-enabled consumer
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "notification-processor", "consumer-3", config)
	if err != nil {
		log.Printf("Failed to create DLQ consumer: %v", err)
		return
	}
	defer consumer.Stop()

	// Simulate processing a notification message
	message := &Message{
		Topic:     "notifications",
		Partition: 0,
		Offset:    300,
		Value:     []byte(`{"user_id": "user_456", "type": "email", "template": "welcome"}`),
	}

	// Define a processor that simulates failures
	failureCount := 0
	processor := func(msg *Message) error {
		failureCount++
		fmt.Printf("Processing notification (attempt %d): %s\n", failureCount, string(msg.Value))
		
		// Simulate failure for first 2 attempts
		if failureCount <= 2 {
			fmt.Printf("Simulated failure: Email service unavailable (attempt %d)\n", failureCount)
			return fmt.Errorf("email service unavailable")
		}
		
		// Success on 3rd attempt
		fmt.Println("Notification sent successfully")
		return nil
	}

	// Process the message multiple times to demonstrate retry logic
	for i := 1; i <= 4; i++ {
		fmt.Printf("\nProcessing attempt %d:\n", i)
		err = consumer.ProcessMessage(message, processor)
		if err != nil {
			fmt.Printf("Processing failed: %v\n", err)
		} else {
			fmt.Println("Processing succeeded")
			break
		}
		
		// Small delay between attempts for demonstration
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("Failure simulation example completed")
}

func processMessageDemo(client *client.Client, config *dlq.DLQConfig) {
	fmt.Println("\n4. ProcessMessage Method Demo")
	fmt.Println("-----------------------------")

	// Create DLQ consumer
	consumer, err := dlq.NewDlqConsumerWithConfig(client, "process-demo", "consumer-4", config)
	if err != nil {
		log.Printf("Failed to create DLQ consumer: %v", err)
		return
	}
	defer consumer.Stop()

	// Demo message
	message := &Message{
		Topic:     "process-demo-topic",
		Partition: 0,
		Offset:    1,
		Value:     []byte("ProcessMessage demo message"),
	}

	// Processor with retry logic demonstration
	attemptCount := 0
	processor := func(msg *Message) error {
		attemptCount++
		fmt.Printf("ProcessMessage attempt %d: %s\n", attemptCount, string(msg.Value))
		
		// Fail first 2 attempts, succeed on 3rd
		if attemptCount <= 2 {
			fmt.Printf("Attempt %d failed (demo)\n", attemptCount)
			return fmt.Errorf("demo failure on attempt %d", attemptCount)
		}
		
		fmt.Printf("Attempt %d succeeded!\n", attemptCount)
		return nil
	}

	// Use ProcessMessage method
	fmt.Println("Demonstrating ProcessMessage with retry logic...")
	err = consumer.ProcessMessage(message, processor)
	if err != nil {
		fmt.Printf("ProcessMessage failed: %v\n", err)
	} else {
		fmt.Printf("ProcessMessage succeeded after %d attempts\n", attemptCount)
	}

	fmt.Println("ProcessMessage demo completed")
}