package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/async"
	"github.com/issac1998/go-queue/internal/protocol"
)

func testLargeResponse() {
	// Create client with async IO enabled
	config := client.ClientConfig{
		BrokerAddrs: []string{"127.0.0.1:9092"},
		Timeout:     5 * time.Second,
		EnableAsyncIO: true,
		AsyncIO: async.AsyncIOConfig{
			WorkerCount: 4,
			SQSize:      1024,
			CQSize:      1024,
			BatchSize:   32,
			PollTimeout: 100 * time.Millisecond,
		},
	}

	c := client.NewClient(config)
	defer c.Close()

	// Discover controller
	err := c.DiscoverController()
	if err != nil {
		log.Fatalf("Failed to discover controller: %v", err)
	}

	fmt.Println("=== Testing Large Response Handling ===")

	// Test with different response sizes
	testSizes := []int{
		1024,      // 1KB
		65536,     // 64KB (previous limit)
		131072,    // 128KB
		1048576,   // 1MB
	}

	for _, size := range testSizes {
		fmt.Printf("\nTesting response size: %d bytes (%.1fKB)\n", size, float64(size)/1024)
		
		// Create a large topic name to simulate large response
		topicName := fmt.Sprintf("large_test_topic_%d_%s", size, generateLargeString(size/100))
		
		start := time.Now()
		done := make(chan bool, 1)
		
		// Build create topic request
		requestData, _ := buildCreateTopicRequest(topicName, 3, 2)
		
		// Send async request with callback
		err := c.AsyncRequestWithCallback(
			"127.0.0.1:9092",
			protocol.CreateTopicRequestType,
			requestData,
			func(responseData []byte) (interface{}, error) {
				// Parse binary response
				if len(responseData) < 4 {
					return nil, fmt.Errorf("response too short")
				}
				errorCode := binary.BigEndian.Uint32(responseData[:4])
				return map[string]interface{}{
					"error_code": errorCode,
					"topic_name": topicName,
					"response_size": len(responseData),
				}, nil
			},
			func(result interface{}, err error) {
				defer func() { done <- true }()
				
				duration := time.Since(start)
				
				if err != nil {
					fmt.Printf("❌ Failed: %v (took %v)\n", err, duration)
					return
				}
				
				if resultMap, ok := result.(map[string]interface{}); ok {
					fmt.Printf("✅ Success: error_code=%v, response_size=%v bytes (took %v)\n", 
						resultMap["error_code"], resultMap["response_size"], duration)
				} else {
					fmt.Printf("✅ Success: %v (took %v)\n", result, duration)
				}
			},
		)
		
		if err != nil {
			fmt.Printf("❌ Failed to send request: %v\n", err)
			continue
		}
		
		// Wait for response with timeout
		select {
		case <-done:
			// Response received
		case <-time.After(10 * time.Second):
			fmt.Printf("❌ Timeout waiting for response\n")
		}
	}

	fmt.Println("\n=== Large Response Test Completed ===")
}



// generateLargeString generates a string of specified length
func generateLargeString(length int) string {
	if length <= 0 {
		return "test"
	}
	
	var buf bytes.Buffer
	pattern := "abcdefghijklmnopqrstuvwxyz0123456789"
	
	for buf.Len() < length {
		remaining := length - buf.Len()
		if remaining >= len(pattern) {
			buf.WriteString(pattern)
		} else {
			buf.WriteString(pattern[:remaining])
		}
	}
	
	return buf.String()
}