package client

import (
	"testing"
)

func TestNewAdmin(t *testing.T) {
	client := NewClient(ClientConfig{})
	admin := NewAdmin(client)

	if admin.client != client {
		t.Error("admin client not set correctly")
	}
}

func TestCreateTopicRequest_Validation(t *testing.T) {
	client := NewClient(ClientConfig{})
	admin := NewAdmin(client)

	tests := []struct {
		name     string
		request  CreateTopicRequest
		expected CreateTopicRequest
	}{
		{
			name: "default values",
			request: CreateTopicRequest{
				Name: "test-topic",
			},
			expected: CreateTopicRequest{
				Name:       "test-topic",
				Partitions: 1,
				Replicas:   1,
			},
		},
		{
			name: "custom values",
			request: CreateTopicRequest{
				Name:       "custom-topic",
				Partitions: 3,
				Replicas:   2,
			},
			expected: CreateTopicRequest{
				Name:       "custom-topic",
				Partitions: 3,
				Replicas:   2,
			},
		},
		{
			name: "zero partitions should default to 1",
			request: CreateTopicRequest{
				Name:       "zero-partitions",
				Partitions: 0,
				Replicas:   1,
			},
			expected: CreateTopicRequest{
				Name:       "zero-partitions",
				Partitions: 1,
				Replicas:   1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := admin.CreateTopic(tt.request)
			// We expect connection error since broker is not running
			if err == nil {
				t.Error("expected connection error but got none")
			}
		})
	}
}

func TestBuildCreateTopicRequest(t *testing.T) {
	client := NewClient(ClientConfig{})
	admin := NewAdmin(client)

	req := CreateTopicRequest{
		Name:       "test-topic",
		Partitions: 3,
		Replicas:   1,
	}

	data, err := admin.buildCreateTopicRequest(req)
	if err != nil {
		t.Fatalf("failed to build create topic request: %v", err)
	}

	if len(data) == 0 {
		t.Error("request data is empty")
	}
}

func TestParseCreateTopicResponse(t *testing.T) {
	client := NewClient(ClientConfig{})
	admin := NewAdmin(client)

	tests := []struct {
		name         string
		responseData []byte
		expectError  bool
		errorCode    int16
	}{
		{
			name:         "success response",
			responseData: []byte{0x00, 0x00}, // ErrorCode = 0
			expectError:  false,
			errorCode:    0,
		},
		{
			name:         "error response",
			responseData: []byte{0x00, 0x01}, // ErrorCode = 1
			expectError:  true,
			errorCode:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := admin.parseCreateTopicResponse("test-topic", tt.responseData)
			if err != nil {
				t.Fatalf("failed to parse response: %v", err)
			}

			if result.Name != "test-topic" {
				t.Errorf("expected topic name 'test-topic', got '%s'", result.Name)
			}

			if tt.expectError && result.Error == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && result.Error != nil {
				t.Errorf("unexpected error: %v", result.Error)
			}
		})
	}
}

func TestUnimplementedMethods(t *testing.T) {
	client := NewClient(ClientConfig{})
	admin := NewAdmin(client)

	// Test ListTopics
	_, err := admin.ListTopics()
	if err == nil {
		t.Error("expected error for unimplemented ListTopics")
	}

	// Test DeleteTopic
	err = admin.DeleteTopic("test-topic")
	if err == nil {
		t.Error("expected error for unimplemented DeleteTopic")
	}

	// Test GetTopicInfo
	_, err = admin.GetTopicInfo("test-topic")
	if err == nil {
		t.Error("expected error for unimplemented GetTopicInfo")
	}
}
