package client

import (
	"testing"
)

func TestNewConsumer(t *testing.T) {
	client := NewClient(ClientConfig{})
	consumer := NewConsumer(client)

	if consumer.client != client {
		t.Error("consumer client not set correctly")
	}
}

func TestFetchRequest_Validation(t *testing.T) {
	client := NewClient(ClientConfig{})
	consumer := NewConsumer(client)

	tests := []struct {
		name    string
		request FetchRequest
	}{
		{
			name: "default max bytes",
			request: FetchRequest{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    0,
				MaxBytes:  0, // Should default to 1MB
			},
		},
		{
			name: "custom max bytes",
			request: FetchRequest{
				Topic:     "test-topic",
				Partition: 0,
				Offset:    100,
				MaxBytes:  2048,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := consumer.Fetch(tt.request)
			// We expect connection error since broker is not running
			if err == nil {
				t.Error("expected connection error but got none")
			}
		})
	}
}

func TestBuildFetchRequest(t *testing.T) {
	client := NewClient(ClientConfig{})
	consumer := NewConsumer(client)

	req := FetchRequest{
		Topic:     "test-topic",
		Partition: 1,
		Offset:    42,
		MaxBytes:  1024,
	}

	data, err := consumer.buildFetchRequest(req)
	if err != nil {
		t.Fatalf("failed to build fetch request: %v", err)
	}

	if len(data) == 0 {
		t.Error("request data is empty")
	}
}

func TestFetchFrom(t *testing.T) {
	client := NewClient(ClientConfig{})
	consumer := NewConsumer(client)

	// Test with default parameters
	_, err := consumer.FetchFrom("test-topic", 0, 0)
	// We expect connection error since broker is not running
	if err == nil {
		t.Error("expected connection error but got none")
	}
}

func TestParseFetchResponse(t *testing.T) {
	client := NewClient(ClientConfig{})
	consumer := NewConsumer(client)

	// Create mock response data with proper structure
	responseData := make([]byte, 50)
	offset := 0

	// Topic length (2 bytes) - "test-topic" = 10 chars
	responseData[offset] = 0x00
	responseData[offset+1] = 0x0A
	offset += 2

	// Topic content (10 bytes)
	copy(responseData[offset:], "test-topic")
	offset += 10

	// Partition (4 bytes)
	responseData[offset] = 0x00
	responseData[offset+1] = 0x00
	responseData[offset+2] = 0x00
	responseData[offset+3] = 0x00 // partition = 0
	offset += 4

	// ErrorCode (2 bytes)
	responseData[offset] = 0x00
	responseData[offset+1] = 0x00 // no error
	offset += 2

	// NextOffset (8 bytes)
	responseData[offset] = 0x00
	responseData[offset+1] = 0x00
	responseData[offset+2] = 0x00
	responseData[offset+3] = 0x00
	responseData[offset+4] = 0x00
	responseData[offset+5] = 0x00
	responseData[offset+6] = 0x00
	responseData[offset+7] = 0x02 // nextOffset = 2
	offset += 8

	// Message count (4 bytes)
	responseData[offset] = 0x00
	responseData[offset+1] = 0x00
	responseData[offset+2] = 0x00
	responseData[offset+3] = 0x01 // 1 message
	offset += 4

	// Message length (4 bytes)
	responseData[offset] = 0x00
	responseData[offset+1] = 0x00
	responseData[offset+2] = 0x00
	responseData[offset+3] = 0x05 // 5 bytes
	offset += 4

	// Message content (5 bytes)
	copy(responseData[offset:], "hello")

	result, err := consumer.parseFetchResponse("test-topic", 0, 0, responseData)
	if err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result.Topic != "test-topic" {
		t.Errorf("expected topic 'test-topic', got '%s'", result.Topic)
	}
	if result.Partition != 0 {
		t.Errorf("expected partition 0, got %d", result.Partition)
	}
	if result.NextOffset != 2 {
		t.Errorf("expected NextOffset 2, got %d", result.NextOffset)
	}
	if len(result.Messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(result.Messages))
	}
	if result.Error != nil {
		t.Errorf("expected no error, got %v", result.Error)
	}

	if len(result.Messages) > 0 {
		msg := result.Messages[0]
		if string(msg.Value) != "hello" {
			t.Errorf("expected message 'hello', got '%s'", string(msg.Value))
		}
		if msg.Offset != 0 {
			t.Errorf("expected message offset 0, got %d", msg.Offset)
		}
	}
}
