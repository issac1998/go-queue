package main

import (
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
)

func main() {
	// Create client configuration
	config := client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092", "127.0.0.1:9093"},
		Timeout:     30 * time.Second,
	}

	// Create client
	c := client.NewClient(config)
	defer c.Close()

	// Discover controller
	log.Println("Discovering controller...")
	if err := c.DiscoverController(); err != nil {
		log.Fatalf("Failed to discover controller: %v", err)
	}
	log.Printf("Controller discovered: %s", c.GetControllerAddr())

	// Create admin client
	admin := client.NewAdmin(c)

	// Create topic request
	req := client.CreateTopicRequest{
		Name:       "test-topic",
		Partitions: 3,
		Replicas:   1,
	}

	// Create topic
	log.Printf("Creating topic: %s with %d partitions", req.Name, req.Partitions)
	result, err := admin.CreateTopic(req)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	if result.Error != nil {
		log.Printf("Topic creation failed: %v", result.Error)
	} else {
		log.Printf("Topic '%s' created successfully", result.Name)
	}

	// List topics to verify
	log.Println("Listing topics...")
	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Failed to list topics: %v", err)
	} else {
		log.Printf("Found %d topics:", len(topics))
		
	}
}
