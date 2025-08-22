package client

import (
	"testing"
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

	client := NewClient(ClientConfig{})
	producer := NewProducer(client)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := producer.SendBatch(tt.messages)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil && err.Error() != "failed to send request: failed to connect to broker: dial tcp [::1]:9092: connect: connection refused" {
				// Ignore connection errors for validation tests
				if err.Error() != "failed to send request: failed to connect to broker: dial tcp 127.0.0.1:9092: connect: connection refused" {
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

	result, err := producer.parseProduceResponse("test-topic", 0, responseData)
	if err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got '%s'", result.Topic)
	}
	if result.Partition != 0 {
		t.Errorf("expected partition 0, got %d", result.Partition)
	}
	if result.Offset != 5 {
		t.Errorf("expected offset 5, got %d", result.Offset)
	}
	if result.Error != nil {
		t.Errorf("expected no error, got %v", result.Error)
	}
}
