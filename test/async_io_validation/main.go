package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	// Create client with sync IO (default)
	clientConfig := client.ClientConfig{
		BrokerAddrs:   []string{"127.0.0.1:9092"},
		EnableAsyncIO: false, // Sync IO
		Timeout:       5 * time.Second,
	}

	clientSync := client.NewClient(clientConfig)
	defer clientSync.Close()

	// Create sync producer
	producerSync := client.NewProducer(clientSync)

	// Send sync message
	msgSync := client.ProduceMessage{
		Topic: "test-topic",
		Key:   []byte("sync-key"),
		Value: []byte("sync-message"),
	}

	result, err := producerSync.Send(msgSync)
	if err != nil {
		log.Printf("Failed to send sync message: %v", err)
	} else {
		fmt.Printf("Sent sync message successfully: %+v\n", result)
	}

	time.Sleep(1 * time.Second)

	// Create client with async IO
	clientConfigAsync := client.ClientConfig{
		BrokerAddrs:   []string{"127.0.0.1:9092"},
		EnableAsyncIO: true, // Async IO
		Timeout:       5 * time.Second,
	}

	clientAsync := client.NewClient(clientConfigAsync)
	defer clientAsync.Close()

	// Create async producer
	producerAsync := client.NewProducer(clientAsync)

	// Send async message
	msgAsync := client.ProduceMessage{
		Topic: "test-topic",
		Key:   []byte("async-key"),
		Value: []byte("async-message"),
	}

	result, err = producerAsync.Send(msgAsync)
	if err != nil {
		log.Printf("Failed to send async message: %v", err)
	} else {
		fmt.Printf("Sent async message successfully: %+v\n", result)
	}

	fmt.Println("Test completed! Check broker logs to verify AsyncIO field handling.")
}