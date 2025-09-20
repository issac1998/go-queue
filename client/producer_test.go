package client

import (
	"strings"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/errors"
)

func TestNewProducer(t *testing.T) {
	client := NewClient(ClientConfig{})
	producer := NewProducer(client)

	if producer.client != client {
		t.Error("producer client not set correctly")
	}
}

func TestProduceMessage_Validation(t *testing.T) {
	tests := []struct {
		name        string
		messages    []ProduceMessage
		expectError bool
	}{
		{
			name:        "empty message list",
			messages:    []ProduceMessage{},
			expectError: true,
		},
		{
			name: "single valid message",
			messages: []ProduceMessage{
				{Topic: "test-topic", Partition: 0, Value: []byte("test message")},
			},
			expectError: false,
		},
		{
			name: "multiple messages same topic/partition",
			messages: []ProduceMessage{
				{Topic: "test-topic", Partition: 0, Value: []byte("message 1")},
				{Topic: "test-topic", Partition: 0, Value: []byte("message 2")},
			},
			expectError: false,
		},
		{
			name: "messages different topics",
			messages: []ProduceMessage{
				{Topic: "topic1", Partition: 0, Value: []byte("message 1")},
				{Topic: "topic2", Partition: 0, Value: []byte("message 2")},
			},
			expectError: true,
		},
		{
			name: "messages different partitions",
			messages: []ProduceMessage{
				{Topic: "test-topic", Partition: 0, Value: []byte("message 1")},
				{Topic: "test-topic", Partition: 1, Value: []byte("message 2")},
			},
			expectError: true,
		},
	}

	client := NewClient(ClientConfig{
		BrokerAddrs: []string{"localhost:19999"}, // Use unlikely port
		Timeout:     50 * time.Millisecond,       // Very short timeout for tests
	})
	producer := NewProducer(client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := producer.SendBatch(tt.messages)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				// For tests that don't expect errors, we expect connection errors since no broker is running
				if !errors.IsConnectionError(err) && !strings.Contains(err.Error(), "failed to") {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestBuildProduceRequest(t *testing.T) {
	client := NewClient(ClientConfig{})
	producer := NewProducer(client)

	messages := []ProduceMessage{
		{Topic: "test-topic", Partition: 0, Value: []byte("test message 1")},
		{Topic: "test-topic", Partition: 0, Value: []byte("test message 2")},
	}

	data, err := producer.buildProduceRequest("test-topic", 0, messages)
	if err != nil {
		t.Fatalf("failed to build produce request: %v", err)
	}

	if len(data) == 0 {
		t.Error("request data is empty")
	}
}

func TestParseProduceResponse(t *testing.T) {
	client := NewClient(ClientConfig{})
	producer := NewProducer(client)

	// Create mock response data
	responseData := make([]byte, 10)
	// BaseOffset (8 bytes)
	responseData[0] = 0x00
	responseData[1] = 0x00
	responseData[2] = 0x00
	responseData[3] = 0x00
	responseData[4] = 0x00
	responseData[5] = 0x00
	responseData[6] = 0x00
	responseData[7] = 0x05 // offset = 5
	// ErrorCode (2 bytes)
	responseData[8] = 0x00 // no error
	responseData[9] = 0x00

	result, err := producer.parseProduceResponse(responseData)
	if err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result.Offset != 5 {
		t.Errorf("expected offset 5, got %d", result.Offset)
	}
}

func TestProducerBatchValidation(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:19999"}, // Use unlikely port
		Timeout:     50 * time.Millisecond,
	}
	client := NewClient(config)
	producer := NewProducer(client)

	// Test empty batch
	_, err := producer.SendBatch([]ProduceMessage{})
	if err == nil {
		t.Error("Expected error for empty batch, got nil")
	}
	if !strings.Contains(err.Error(), "cannot be empty") {
		t.Errorf("Expected 'cannot be empty' error, got: %v", err)
	}

	// Test mixed topic/partition batch
	mixedMessages := []ProduceMessage{
		{Topic: "topic1", Partition: 0, Value: []byte("msg1")},
		{Topic: "topic2", Partition: 0, Value: []byte("msg2")}, // Different topic
	}
	_, err = producer.SendBatch(mixedMessages)
	if err == nil {
		t.Error("Expected error for mixed topic batch, got nil")
	}
	if !strings.Contains(err.Error(), "same topic") {
		t.Errorf("Expected 'same topic' error, got: %v", err)
	}

	// Test mixed partition batch
	mixedPartitions := []ProduceMessage{
		{Topic: "topic1", Partition: 0, Value: []byte("msg1")},
		{Topic: "topic1", Partition: 1, Value: []byte("msg2")}, // Different partition
	}
	_, err = producer.SendBatch(mixedPartitions)
	if err == nil {
		t.Error("Expected error for mixed partition batch, got nil")
	}
	if !strings.Contains(err.Error(), "same topic and partition") {
		t.Errorf("Expected 'same topic and partition' error, got: %v", err)
	}
}
