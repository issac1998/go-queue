package main

import (
	"fmt"
	"time"

	"github.com/issac1998/go-queue/client"
)

func testProducerRetry() {
	fmt.Println("\n=== Testing Producer Retry Mechanism ===")
	
	// Create client and producer
	clientInstance := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092"},
		Timeout:     time.Second * 10,
	})
	producer := client.NewProducer(clientInstance)

	// Test producing messages
	for i := 0; i < 5; i++ {
		message := fmt.Sprintf("Test message %d from retry test", i)
		produceMsg := client.ProduceMessage{
			Topic:     "test-topic",
			Partition: 0,
			Value:     []byte(message),
		}
		result, err := producer.SendBatch([]client.ProduceMessage{produceMsg})
		if err != nil {
			fmt.Printf("Failed to produce message %d: %v\n", i, err)
		} else if result != nil {
			fmt.Printf("Successfully produced message %d at offset %d\n", i, result.Offset)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func testConsumerRetry() {
	fmt.Println("\n=== Testing Consumer Retry Mechanism ===")
	
	// Create client and consumer
	clientInstance := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092"},
		Timeout:     time.Second * 10,
	})
	consumer := client.NewConsumer(clientInstance)

	// Test consuming messages
	for i := 0; i < 3; i++ {
		fetchReq := client.FetchRequest{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    int64(i),
			MaxBytes:  1024,
		}
		result, err := consumer.Fetch(fetchReq)
		if err != nil {
			fmt.Printf("Failed to fetch message at offset %d: %v\n", i, err)
		} else if len(result.Messages) > 0 {
			fmt.Printf("Successfully fetched message at offset %d: %s\n", i, string(result.Messages[0].Value))
		} else {
			fmt.Printf("No message found at offset %d\n", i)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func testBatchConsumerRetry() {
	fmt.Println("\n=== Testing Batch Consumer Retry Mechanism ===")
	
	// Create client and consumer
	clientInstance := client.NewClient(client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092"},
		Timeout:     time.Second * 10,
	})
	consumer := client.NewConsumer(clientInstance)

	// Test batch consuming messages
	ranges := []client.FetchRange{
		{Offset: 0, MaxBytes: 1024, MaxCount: 2},
		{Offset: 2, MaxBytes: 1024, MaxCount: 2},
	}
	
	req := client.BatchFetchRequest{
		Topic:     "test-topic",
		Partition: 0,
		Ranges:    ranges,
	}
	
	result, err := consumer.BatchFetch(req)
	if err != nil {
		fmt.Printf("Failed to batch fetch messages: %v\n", err)
	} else {
		fmt.Printf("Successfully batch fetched %d message ranges\n", len(result.Results))
		for i, rangeResult := range result.Results {
			fmt.Printf("Range %d: %d messages\n", i, len(rangeResult.Messages))
			for j, msg := range rangeResult.Messages {
				fmt.Printf("  Message %d: %s\n", j, string(msg.Value))
			}
		}
	}
}

func testRetryMechanisms() {
	fmt.Println("Starting retry mechanism tests...")
	
	// Wait a bit for broker to be ready
	time.Sleep(time.Second * 2)
	
	// Test producer retry
	testProducerRetry()
	
	// Wait a bit between tests
	time.Sleep(time.Second * 1)
	
	// Test consumer retry
	testConsumerRetry()
	
	// Wait a bit between tests
	time.Sleep(time.Second * 1)
	
	// Test batch consumer retry
	testBatchConsumerRetry()
	
	fmt.Println("\nRetry mechanism tests completed!")
}