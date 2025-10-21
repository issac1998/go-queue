package main

import (
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/testutil"
)

func main() {
	fmt.Println("=== Starting Simple Cluster Test ===")
	
	cluster := testutil.NewTestCluster()
	defer func() {
		log.Println("Stopping test cluster...")
		cluster.Stop()
		log.Println("Test cluster stopped")
	}()

	log.Println("Starting test cluster...")
	if err := cluster.Start(); err != nil {
		log.Fatalf("Failed to start test cluster: %v", err)
	}
	
	log.Println("Test cluster started successfully!")
	
	// Wait a bit to ensure everything is stable
	time.Sleep(5 * time.Second)
	
	// Try to get broker addresses
	addresses := cluster.GetAllBrokerAddresses()
	log.Printf("Broker addresses: %v", addresses)
	
	if len(addresses) == 0 {
		log.Fatalf("No broker addresses found")
	}
	
	// Try to create a simple topic
	topicName := "simple-test-topic"
	log.Printf("Creating topic: %s", topicName)
	if err := cluster.CreateTopic(topicName, 1, 1); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	
	log.Printf("Topic %s created successfully!", topicName)
	
	// Clean up topic
	log.Printf("Deleting topic: %s", topicName)
	if err := cluster.DeleteTopic(topicName); err != nil {
		log.Printf("Warning: Failed to delete topic: %v", err)
	}
	
	log.Println("=== Simple Cluster Test Completed Successfully ===")
}