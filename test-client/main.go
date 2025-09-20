package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	// Connect to the broker
	fmt.Println("Connecting to broker at 127.0.0.1:9092...")
	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		fmt.Printf("Failed to connect to broker: %v\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connected successfully!")

	// Create GetTopicMetadata request
	request := createGetTopicMetadataRequest("test-topic")

	// Send request
	if _, err := conn.Write(request); err != nil {
		fmt.Printf("Failed to send request: %v\n", err)
		return
	}

	// Read response length first
	var responseLen int32
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		fmt.Printf("Failed to read response length: %v\n", err)
		return
	}
	fmt.Printf("Response length: %d\n", responseLen)

	// Read response data
	response := make([]byte, responseLen)
	if _, err := conn.Read(response); err != nil {
		fmt.Printf("Failed to read response data: %v\n", err)
		return
	}

	// Parse response
	parseGetTopicMetadataResponse(response)
	
	// Test retry mechanisms
	testRetryMechanisms()
}

func createGetTopicMetadataRequest(topicName string) []byte {
	buf := new(bytes.Buffer)

	// Request type (GetTopicMetadata = 1002)
	binary.Write(buf, binary.BigEndian, int32(1002))

	// Create payload (topic name length + topic name)
	payload := new(bytes.Buffer)
	binary.Write(payload, binary.BigEndian, int32(len(topicName)))
	payload.Write([]byte(topicName))

	// Data length
	binary.Write(buf, binary.BigEndian, int32(payload.Len()))

	// Payload data
	buf.Write(payload.Bytes())

	return buf.Bytes()
}

func parseGetTopicMetadataResponse(data []byte) {
	buf := bytes.NewReader(data)

	// Read error code
	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		fmt.Printf("Failed to read error code: %v\n", err)
		return
	}
	fmt.Printf("Error code: %d\n", errorCode)

	if errorCode != 0 {
		fmt.Printf("Request failed with error code: %d\n", errorCode)
		return
	}

	// Read partition count
	var partitionCount int32
	if err := binary.Read(buf, binary.BigEndian, &partitionCount); err != nil {
		fmt.Printf("Failed to read partition count: %v\n", err)
		return
	}
	fmt.Printf("Partition count: %d\n", partitionCount)

	// Read each partition's metadata
	for i := int32(0); i < partitionCount; i++ {
		fmt.Printf("\nPartition %d:\n", i)

		// Read partition ID
		var partitionID int32
		if err := binary.Read(buf, binary.BigEndian, &partitionID); err != nil {
			fmt.Printf("Failed to read partition ID: %v\n", err)
			return
		}
		fmt.Printf("  Partition ID: %d\n", partitionID)

		// Read leader address length
		var leaderAddrLen int32
		if err := binary.Read(buf, binary.BigEndian, &leaderAddrLen); err != nil {
			fmt.Printf("Failed to read leader address length: %v\n", err)
			return
		}

		// Read leader address
		leaderAddr := make([]byte, leaderAddrLen)
		if _, err := buf.Read(leaderAddr); err != nil {
			fmt.Printf("Failed to read leader address: %v\n", err)
			return
		}
		fmt.Printf("  Leader address: %s\n", string(leaderAddr))

		// Read replica count
		var replicaCount int32
		if err := binary.Read(buf, binary.BigEndian, &replicaCount); err != nil {
			fmt.Printf("Failed to read replica count: %v\n", err)
			return
		}
		fmt.Printf("  Replica count: %d\n", replicaCount)

		// Read each replica address
		for j := int32(0); j < replicaCount; j++ {
			// Read replica address length
			var replicaAddrLen int32
			if err := binary.Read(buf, binary.BigEndian, &replicaAddrLen); err != nil {
				fmt.Printf("Failed to read replica address length: %v\n", err)
				return
			}

			// Read replica address
			replicaAddr := make([]byte, replicaAddrLen)
			if _, err := buf.Read(replicaAddr); err != nil {
				fmt.Printf("Failed to read replica address: %v\n", err)
				return
			}
			fmt.Printf("  Replica %d address: %s\n", j, string(replicaAddr))
		}
	}
}