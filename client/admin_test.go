package client

import (
	"strings"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

func TestNewAdmin(t *testing.T) {
	client := NewClient(ClientConfig{})
	admin := NewAdmin(client)

	if admin.client != client {
		t.Error("admin client not set correctly")
	}
}

func TestCreateTopicRequest_Validation(t *testing.T) {
	// Use very short timeout for tests to avoid hanging
	client := NewClient(ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     50 * time.Millisecond, // Very short timeout for tests
	})
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
	client := NewClient(ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     50 * time.Millisecond, // Very short timeout for tests
	})
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

func TestCreateTopic(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     50,
	}
	client := NewClient(config)
	admin := NewAdmin(client)

	createReq := CreateTopicRequest{
		Name:       "test-topic",
		Partitions: 3,
		Replicas:   1,
	}

	_, err := admin.CreateTopic(createReq)
	if err == nil {
		t.Error("Expected error when no broker is running, got nil")
	}

	// Should contain connection-related error
	if !strings.Contains(err.Error(), "failed to") {
		t.Errorf("Expected connection error, got: %v", err)
	}
}


func TestClientFollowerReadClassification(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     50,
	}
	client := NewClient(config)

	// Test that our read/write classification works
	testCases := []struct {
		requestType int32
		isWrite     bool
		description string
	}{
		{protocol.CreateTopicRequestType, true, "CREATE_TOPIC"},
		{protocol.ListTopicsRequestType, false, "LIST_TOPICS"},
		{protocol.JoinGroupRequestType, true, "JOIN_GROUP"},
		{protocol.GetTopicMetadataRequestType, false, "GET_TOPIC_METADATA"},
	}

	for _, tc := range testCases {
		result := client.isMetadataWriteRequest(tc.requestType)
		if result != tc.isWrite {
			t.Errorf("Expected %s (type %d) to be write=%v, got write=%v",
				tc.description, tc.requestType, tc.isWrite, result)
		}
	}
}
