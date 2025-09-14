package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

func main() {
	// Connect to broker
	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to broker")

	// Test produce request
	if err := testProduce(conn); err != nil {
		log.Fatalf("Produce test failed: %v", err)
	}

	fmt.Println("All tests passed!")
}

func testProduce(conn net.Conn) error {
	fmt.Println("Testing produce request...")

	// Build produce request
	request := buildProduceRequest("test-topic", 0, []Message{
		{Key: "key1", Value: "Hello World 1"},
		{Key: "key2", Value: "Hello World 2"},
	})

	// Send request type (ProduceRequestType = 1)
	requestType := make([]byte, 4)
	binary.BigEndian.PutUint32(requestType, 1)
	if _, err := conn.Write(requestType); err != nil {
		return fmt.Errorf("failed to write request type: %v", err)
	}

	// Send request length
	requestLength := make([]byte, 4)
	binary.BigEndian.PutUint32(requestLength, uint32(len(request)))
	if _, err := conn.Write(requestLength); err != nil {
		return fmt.Errorf("failed to write request length: %v", err)
	}

	// Send request data
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("failed to write request data: %v", err)
	}

	fmt.Println("Sent produce request")

	// Read response length
	var responseLength uint32
	if err := binary.Read(conn, binary.BigEndian, &responseLength); err != nil {
		return fmt.Errorf("failed to read response length: %v", err)
	}

	// Read response data
	responseData := make([]byte, responseLength)
	if _, err := conn.Read(responseData); err != nil {
		return fmt.Errorf("failed to read response data: %v", err)
	}

	fmt.Printf("Received response: %d bytes\n", len(responseData))

	// Parse response
	response, err := parseProduceResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse response: %v", err)
	}

	fmt.Printf("Response: Topic=%s, Partition=%d, ErrorCode=%d, Error=%s\n",
		response.Topic, response.Partition, response.ErrorCode, response.Error)

	for i, result := range response.Results {
		fmt.Printf("  Result %d: Offset=%d, Timestamp=%d, Error=%s\n",
			i, result.Offset, result.Timestamp, result.Error)
	}

	return nil
}

type Message struct {
	Key   string
	Value string
}

type ProduceResult struct {
	Offset    int64
	Timestamp int64
	Error     string
}

type ProduceResponse struct {
	Topic     string
	Partition int32
	ErrorCode int32
	Error     string
	Results   []ProduceResult
}

func buildProduceRequest(topic string, partition int32, messages []Message) []byte {
	var request []byte

	// Write topic length and topic
	topicBytes := []byte(topic)
	topicLen := make([]byte, 4)
	binary.BigEndian.PutUint32(topicLen, uint32(len(topicBytes)))
	request = append(request, topicLen...)
	request = append(request, topicBytes...)

	// Write partition
	partitionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(partitionBytes, uint32(partition))
	request = append(request, partitionBytes...)

	// Write message count
	messageCount := make([]byte, 4)
	binary.BigEndian.PutUint32(messageCount, uint32(len(messages)))
	request = append(request, messageCount...)

	// Write each message
	for _, msg := range messages {
		// Write key length and key
		keyBytes := []byte(msg.Key)
		keyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(keyLen, uint32(len(keyBytes)))
		request = append(request, keyLen...)
		request = append(request, keyBytes...)

		// Write value length and value
		valueBytes := []byte(msg.Value)
		valueLen := make([]byte, 4)
		binary.BigEndian.PutUint32(valueLen, uint32(len(valueBytes)))
		request = append(request, valueLen...)
		request = append(request, valueBytes...)
	}

	return request
}

func parseProduceResponse(data []byte) (*ProduceResponse, error) {
	offset := 0

	// Read topic length
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for topic length")
	}
	topicLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read topic
	if len(data) < offset+int(topicLen) {
		return nil, fmt.Errorf("insufficient data for topic")
	}
	topic := string(data[offset : offset+int(topicLen)])
	offset += int(topicLen)

	// Read partition
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for partition")
	}
	partition := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Read error code
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for error code")
	}
	errorCode := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Read error message length
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for error message length")
	}
	errorMsgLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read error message
	if len(data) < offset+int(errorMsgLen) {
		return nil, fmt.Errorf("insufficient data for error message")
	}
	errorMsg := string(data[offset : offset+int(errorMsgLen)])
	offset += int(errorMsgLen)

	// Read results count
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for results count")
	}
	resultsCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read results
	results := make([]ProduceResult, resultsCount)
	for i := uint32(0); i < resultsCount; i++ {
		// Read offset
		if len(data) < offset+8 {
			return nil, fmt.Errorf("insufficient data for result offset")
		}
		resultOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// Read timestamp
		if len(data) < offset+8 {
			return nil, fmt.Errorf("insufficient data for result timestamp")
		}
		timestamp := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8

		// Read error message length
		if len(data) < offset+4 {
			return nil, fmt.Errorf("insufficient data for result error message length")
		}
		resultErrorMsgLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Read error message
		if len(data) < offset+int(resultErrorMsgLen) {
			return nil, fmt.Errorf("insufficient data for result error message")
		}
		resultErrorMsg := string(data[offset : offset+int(resultErrorMsgLen)])
		offset += int(resultErrorMsgLen)

		results[i] = ProduceResult{
			Offset:    resultOffset,
			Timestamp: timestamp,
			Error:     resultErrorMsg,
		}
	}

	return &ProduceResponse{
		Topic:     topic,
		Partition: partition,
		ErrorCode: errorCode,
		Error:     errorMsg,
		Results:   results,
	}, nil
}
