package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/protocol"
)

func main() {
	// Initialize client with async IO enabled
	clientConfig := client.ClientConfig{
		BrokerAddrs:   []string{"127.0.0.1:9092"},
		Timeout:       5 * time.Second,
		EnableAsyncIO: true,
	}

	c := client.NewClient(clientConfig)
	defer c.Close()

	// Discover controller
	if err := c.DiscoverController(); err != nil {
		log.Fatalf("Failed to discover controller: %v", err)
	}

	fmt.Println("=== Async IO Correct Usage Demo ===")

	// Example 2: Callback mode (async with response handling)
	fmt.Println("\n2. Callback mode (async with response handling):")
	demoCallbackMode(c)

	// Example 3: Batch async operations
	fmt.Println("\n3. Batch async operations:")
	demoBatchAsync(c)

	// Wait a bit to see async results
	time.Sleep(2 * time.Second)
	fmt.Println("=== Demo completed ===")

	// Test large response handling
	testLargeResponse()
}

// Callback mode: handle responses asynchronously via callbacks
func demoCallbackMode(c *client.Client) {
	topicName := "async-callback-topic"

	// Build binary protocol request data
	requestData, err := buildCreateTopicRequest(topicName, 2, 1)
	if err != nil {
		log.Printf("Failed to build create topic request: %v", err)
		return
	}

	// Response handler for binary protocol
	handler := func(responseData []byte) (interface{}, error) {
		if len(responseData) < 2 {
			return nil, fmt.Errorf("response too short")
		}

		// Parse binary response (error code)
		buf := bytes.NewReader(responseData)
		var errorCode int16
		if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
			return nil, fmt.Errorf("failed to read error code: %v", err)
		}

		if errorCode != 0 {
			return nil, fmt.Errorf("server returned error code: %d", errorCode)
		}

		return map[string]interface{}{
			"topic_name": topicName,
			"error_code": errorCode,
			"status":     "success",
		}, nil
	}

	// Callback function
	callback := func(result interface{}, err error) {
		if err != nil {
			log.Printf("❌ Callback received error: %v", err)
		} else {
			fmt.Printf("✓ Callback received response: %+v\n", result)
		}
	}

	// Send request with callback
	start := time.Now()
	err = c.AsyncRequestWithCallback(c.GetControllerAddr(), protocol.CreateTopicRequestType, requestData, handler, callback)
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("Callback request failed: %v", err)
	} else {
		fmt.Printf("✓ Callback request sent in %v (response will be handled asynchronously)\n", elapsed)
	}
}

// buildCreateTopicRequest builds binary protocol create topic request
func buildCreateTopicRequest(topicName string, partitions, replicas int32) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(topicName))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topicName); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, partitions); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, replicas); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Batch async operations: send multiple requests concurrently
func demoBatchAsync(c *client.Client) {
	var wg sync.WaitGroup
	numRequests := 5

	start := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			topicName := fmt.Sprintf("batch-topic-%d", index)
			createTopicData := map[string]interface{}{
				"topic_name":         topicName,
				"partitions":         int32(1),
				"replication_factor": int32(1),
			}

			requestData, err := json.Marshal(createTopicData)
			if err != nil {
				log.Printf("Failed to marshal request %d: %v", index, err)
				return
			}

			// Use callback mode for batch operations
			handler := func(responseData []byte) (interface{}, error) {
				var response map[string]interface{}
				if err := json.Unmarshal(responseData, &response); err != nil {
					return nil, fmt.Errorf("failed to unmarshal response: %v", err)
				}
				return response, nil
			}

			callback := func(result interface{}, err error) {
				if err != nil {
					log.Printf("❌ Batch request %d failed: %v", index, err)
				} else {
					fmt.Printf("✓ Batch request %d completed: %s\n", index, topicName)
				}
			}

			err = c.AsyncRequestWithCallback(c.GetControllerAddr(), protocol.CreateTopicRequestType, requestData, handler, callback)
			if err != nil {
				log.Printf("Failed to send batch request %d: %v", index, err)
			}
		}(i)
	}

	// Wait for all requests to be sent (not for responses)
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ All %d batch requests sent in %v (responses handled asynchronously)\n", numRequests, elapsed)
}
