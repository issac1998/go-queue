package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	// Connect to broker
	conn, err := net.Dial("tcp", "127.0.0.1:9092")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Set timeouts
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// Send CreateTopic request
	requestType := int32(4) // CreateTopicRequestType
	binary.Write(conn, binary.BigEndian, requestType)

	// Build request data
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int16(1)) // version
	topicName := "debug-topic"
	binary.Write(buf, binary.BigEndian, int16(len(topicName)))
	buf.WriteString(topicName)
	binary.Write(buf, binary.BigEndian, int32(1)) // partitions
	binary.Write(buf, binary.BigEndian, int32(1)) // replication factor

	requestData := buf.Bytes()
	requestLen := int32(len(requestData))
	binary.Write(conn, binary.BigEndian, requestLen)
	conn.Write(requestData)

	fmt.Printf("Sent request: type=%d, len=%d\n", requestType, requestLen)

	// Read response length
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		fmt.Printf("Failed to read response length: %v\n", err)
		return
	}
	fmt.Printf("Response length: %d\n", responseLen)

	// Read response data
	responseData := make([]byte, responseLen)
	if _, err := conn.Read(responseData); err != nil {
		fmt.Printf("Failed to read response data: %v\n", err)
		return
	}

	fmt.Printf("Response data (%d bytes): %v\n", len(responseData), responseData)
	fmt.Printf("Response data as hex: %x\n", responseData)
	fmt.Printf("Response data as string: %q\n", string(responseData))

	// Try to parse as int16 error code
	if len(responseData) >= 2 {
		buf := bytes.NewReader(responseData)
		var errorCode int16
		if err := binary.Read(buf, binary.BigEndian, &errorCode); err == nil {
			fmt.Printf("Error code (first 2 bytes): %d\n", errorCode)
		}
	}
}