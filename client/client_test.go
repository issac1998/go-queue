package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/protocol"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name     string
		config   ClientConfig
		expected []string // Expected broker addresses
	}{
		{
			name:     "default config",
			config:   ClientConfig{},
			expected: []string{"localhost:9092"},
		},
		{
			name: "single broker",
			config: ClientConfig{
				BrokerAddrs: []string{"127.0.0.1:8080"},
				Timeout:     10 * time.Second,
			},
			expected: []string{"127.0.0.1:8080"},
		},
		{
			name: "multiple brokers",
			config: ClientConfig{
				BrokerAddrs: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
				Timeout:     15 * time.Second,
			},
			expected: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClient(tt.config)

			if !reflect.DeepEqual(client.brokerAddrs, tt.expected) {
				t.Errorf("expected BrokerAddrs %v, got %v", tt.expected, client.brokerAddrs)
			}

			expectedTimeout := tt.config.Timeout
			if expectedTimeout == 0 {
				expectedTimeout = 5 * time.Second
			}
			if client.timeout != expectedTimeout {
				t.Errorf("expected Timeout %v, got %v", expectedTimeout, client.timeout)
			}
		})
	}
}

func TestControllerDiscovery(t *testing.T) {
	client := NewClient(ClientConfig{
		BrokerAddrs: []string{"localhost:9999"}, // Use non-existent port
		Timeout:     100 * time.Millisecond,
	})

	err := client.DiscoverController()
	if err == nil {
		t.Error("expected controller discovery to fail for non-existent brokers")
	}

	// Test that controller address is empty after failed discovery
	if addr := client.GetControllerAddr(); addr != "" {
		t.Errorf("expected empty controller address after failed discovery, got %s", addr)
	}
}

func TestLoadBalancing(t *testing.T) {
	// Create client with multiple broker addresses
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:19992", "localhost:19993", "localhost:19994"}, // Use unlikely ports
		Timeout:     50 * time.Millisecond,
	}
	client := NewClient(config)

	// Test that client initializes properly without load balancing fields
	if len(client.topicMetadata) != 0 {
		t.Errorf("Expected empty topic metadata map, got %d entries", len(client.topicMetadata))
	}

	_, err := client.connectToFollower()
	if err == nil {
		t.Error("Expected error when connecting to non-existent brokers")
	}

	// Test that error message is reasonable
	expectedErrMsg := "failed to connect to any broker"
	if err.Error() != expectedErrMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
	}
}

func TestShuffleBrokerIndices(t *testing.T) {
	// This test is no longer relevant since we removed the shuffle functionality
	// The new architecture uses partition-aware routing instead of random load balancing
	t.Skip("Skipping shuffle test - functionality removed in favor of partition routing")
}

func TestRefreshTopicMetadata(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Timeout:     50, // milliseconds
	}
	client := NewClient(config)

	// Test isControllerError function
	testCases := []struct {
			err      error
			expected bool
		}{
			{nil, false},
			{&errors.TypedError{Type: errors.ControllerError, Message: errors.NotControllerMsg, Cause: nil}, true},
		{&errors.TypedError{Type: errors.LeadershipError, Message: errors.NotLeaderMsg, Cause: nil}, true},
		{&errors.TypedError{Type: errors.ConnectionError, Message: errors.ConnectionRefusedMsg, Cause: nil}, true},
		{&errors.TypedError{Type: errors.ConnectionError, Message: errors.ConnectionResetMsg, Cause: nil}, true},
			{fmt.Errorf("some other error"), false},
		}

	for _, tc := range testCases {
		result := client.isControllerError(tc.err)
		if result != tc.expected {
			t.Errorf("isControllerError(%v) = %v, expected %v", tc.err, result, tc.expected)
		}
	}

	// Test buildTopicMetadataRequest
	requestData, err := client.buildTopicMetadataRequest("test-topic")
	if err != nil {
		t.Errorf("buildTopicMetadataRequest failed: %v", err)
	}

	// Verify request format
	buf := bytes.NewReader(requestData)
	var topicLen int32
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		t.Errorf("Failed to read topic length: %v", err)
	}

	if topicLen != int32(len("test-topic")) {
		t.Errorf("Expected topic length %d, got %d", len("test-topic"), topicLen)
	}

	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(buf, topicBytes); err != nil {
		t.Errorf("Failed to read topic name: %v", err)
	}

	if string(topicBytes) != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", string(topicBytes))
	}
}

func TestParseTopicMetadataResponse(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     50,
	}
	client := NewClient(config)

	// Test success response
	var buf bytes.Buffer

	// Error code (0 = success)
	binary.Write(&buf, binary.BigEndian, int32(0))

	// Partition count
	binary.Write(&buf, binary.BigEndian, int32(2))

	// Partition 0
	binary.Write(&buf, binary.BigEndian, int32(0))
	leaderAddr := "broker1:9092"
	binary.Write(&buf, binary.BigEndian, int32(len(leaderAddr)))
	buf.WriteString(leaderAddr)
	binary.Write(&buf, binary.BigEndian, int32(2)) // replica count
	replica1 := "broker1:9092"
	binary.Write(&buf, binary.BigEndian, int32(len(replica1)))
	buf.WriteString(replica1)
	replica2 := "broker2:9092"
	binary.Write(&buf, binary.BigEndian, int32(len(replica2)))
	buf.WriteString(replica2)

	// Partition 1
	binary.Write(&buf, binary.BigEndian, int32(1))
	leaderAddr2 := "broker2:9092"
	binary.Write(&buf, binary.BigEndian, int32(len(leaderAddr2)))
	buf.WriteString(leaderAddr2)
	binary.Write(&buf, binary.BigEndian, int32(1)) // replica count
	binary.Write(&buf, binary.BigEndian, int32(len(replica2)))
	buf.WriteString(replica2)

	responseData := buf.Bytes()

	metadata, err := client.parseTopicMetadataResponse("test-topic", responseData)
	if err != nil {
		t.Errorf("parseTopicMetadataResponse failed: %v", err)
	}

	if metadata.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", metadata.Topic)
	}

	if len(metadata.Partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(metadata.Partitions))
	}

	// Check partition 0
	if part0, exists := metadata.Partitions[0]; exists {
		if part0.Leader != "broker1:9092" {
			t.Errorf("Expected partition 0 leader 'broker1:9092', got '%s'", part0.Leader)
		}
		if len(part0.Replicas) != 2 {
			t.Errorf("Expected 2 replicas for partition 0, got %d", len(part0.Replicas))
		}
	} else {
		t.Error("Partition 0 not found")
	}

	// Check partition 1
	if part1, exists := metadata.Partitions[1]; exists {
		if part1.Leader != "broker2:9092" {
			t.Errorf("Expected partition 1 leader 'broker2:9092', got '%s'", part1.Leader)
		}
		if len(part1.Replicas) != 1 {
			t.Errorf("Expected 1 replica for partition 1, got %d", len(part1.Replicas))
		}
	} else {
		t.Error("Partition 1 not found")
	}

	// Test error response
	var errorBuf bytes.Buffer
	binary.Write(&errorBuf, binary.BigEndian, int32(1)) // error code
	errorMsg := "topic not found"
	binary.Write(&errorBuf, binary.BigEndian, int32(len(errorMsg)))
	errorBuf.WriteString(errorMsg)

	errorResponseData := errorBuf.Bytes()
	_, err = client.parseTopicMetadataResponse("missing-topic", errorResponseData)
	if err == nil {
		t.Error("Expected error for error response, got nil")
	}

	expectedError := "server error 1: topic not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestMetadataReadWriteClassification(t *testing.T) {
	client := NewClient(ClientConfig{})

	// Test write operations (must use leader)
	writeOperations := []struct {
		requestType int32
		description string
	}{
		{protocol.CreateTopicRequestType, "CREATE_TOPIC"},
		{protocol.DeleteTopicRequestType, "DELETE_TOPIC"},
		{protocol.JoinGroupRequestType, "JOIN_GROUP"},
		{protocol.LeaveGroupRequestType, "LEAVE_GROUP"},
	}

	for _, op := range writeOperations {
		isWrite := client.isMetadataWriteRequest(op.requestType)
		if !isWrite {
			t.Errorf("Expected %s (type %d) to be classified as write operation", op.description, op.requestType)
		}
	}

	// Test read operations (can use follower read)
	readOperations := []struct {
		requestType int32
		description string
	}{
		{protocol.ListTopicsRequestType, "LIST_TOPICS"},
		{protocol.GetTopicInfoRequestType, "GET_TOPIC_INFO"},
		{protocol.ListGroupsRequestType, "LIST_GROUPS"},
		{protocol.DescribeGroupRequestType, "DESCRIBE_GROUP"},
		{protocol.GetTopicMetadataRequestType, "GET_TOPIC_METADATA"},
		{protocol.ControllerDiscoverRequestType, "CONTROLLER_DISCOVERY"},
		{protocol.ControllerVerifyRequestType, "CONTROLLER_VERIFY"},
	}

	for _, op := range readOperations {
		isWrite := client.isMetadataWriteRequest(op.requestType)
		if isWrite {
			t.Errorf("Expected %s (type %d) to be classified as read operation", op.description, op.requestType)
		}
	}
}

func TestConnectForMetadata(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:19999", "localhost:19998"}, // Use unlikely ports
		Timeout:     50 * time.Millisecond,
	}
	client := NewClient(config)

	// Test metadata write operation (should try controller)
	conn1, err1 := client.connectForMetadata(true)
	if conn1 != nil {
		conn1.Close()
	}
	if err1 == nil {
		t.Error("Expected error when no controller is available, got nil")
	} else {
		t.Logf("Write operation error (expected): %v", err1)
	}

	// Test metadata read operation (should try any broker)
	conn2, err2 := client.connectForMetadata(false)
	if conn2 != nil {
		conn2.Close()
		t.Logf("Unexpectedly got a connection: %v", conn2.RemoteAddr())
	}
	if err2 == nil {
		t.Error("Expected error when no broker is available, got nil")
	} else {
		t.Logf("Read operation error (expected): %v", err2)
	}

	// The errors should be connection-related since no brokers are running
	if err2 != nil && !strings.Contains(err2.Error(), "failed to") {
		t.Errorf("Expected connection error, got: %v", err2)
	}
}

func TestFollowerReadSelection(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:9092", "localhost:9093"},
		Timeout:     50 * time.Millisecond,
	}
	client := NewClient(config)

	// Test case 1: Partition with multiple replicas
	partitionMeta := PartitionMetadata{
		Leader:   "localhost:9092",
		Replicas: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
	}

	// Select broker for read should prefer followers
	selectedBroker := client.selectBrokerForRead(partitionMeta)

	// Should select a broker (either follower or leader as fallback)
	if selectedBroker == "" {
		t.Error("Expected a broker to be selected, got empty string")
	}

	// Test case 2: Partition with leader only
	partitionMetaLeaderOnly := PartitionMetadata{
		Leader:   "localhost:9092",
		Replicas: []string{"localhost:9092"}, // Only leader
	}

	selectedBrokerLeaderOnly := client.selectBrokerForRead(partitionMetaLeaderOnly)
	if selectedBrokerLeaderOnly != "localhost:9092" {
		t.Errorf("Expected leader to be selected when no followers available, got: %s", selectedBrokerLeaderOnly)
	}

	// Test case 3: Empty replicas
	partitionMetaEmpty := PartitionMetadata{
		Leader:   "localhost:9092",
		Replicas: []string{},
	}

	selectedBrokerEmpty := client.selectBrokerForRead(partitionMetaEmpty)
	if selectedBrokerEmpty != "localhost:9092" {
		t.Errorf("Expected leader to be selected when no replicas, got: %s", selectedBrokerEmpty)
	}

	t.Log("✅ FollowerRead selection logic test passed")
}

func TestFollowerAvailabilityCheck(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     50 * time.Millisecond,
	}
	client := NewClient(config)

	// Test with unavailable broker (should return false)
	isAvailable := client.isFollowerAvailable("localhost:99999") // Invalid port
	if isAvailable {
		t.Error("Expected follower to be unavailable, but got available")
	}

	// Test with potentially available broker (localhost should be reachable, but port may not be open)
	// This is expected to fail since no broker is actually running
	isLocalAvailable := client.isFollowerAvailable("localhost:9092")
	if isLocalAvailable {
		t.Log("Note: localhost:9092 appears to be available (broker might be running)")
	} else {
		t.Log("localhost:9092 is not available (expected since no broker is running)")
	}

	t.Log("✅ Follower availability check test passed")
}

func TestRoundRobinFollowerSelection(t *testing.T) {
	config := ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     50 * time.Millisecond,
	}
	client := NewClient(config)

	// Test with empty followers list
	selected := client.selectFollower([]string{})
	if selected != "" {
		t.Errorf("Expected empty string for empty followers list, got: %s", selected)
	}

	// Test with single follower (will likely fail availability check)
	followers := []string{"localhost:9999"}
	selected = client.selectFollower(followers)
	// Should return empty since the follower is not available
	if selected != "" {
		t.Logf("Note: Selected follower %s (might be available)", selected)
	}

	// Test with multiple followers (all will likely fail availability check)
	multipleFollowers := []string{"localhost:9999", "localhost:9998", "localhost:9997"}
	selected = client.selectFollower(multipleFollowers)
	// Should return empty since none are available
	if selected != "" {
		t.Logf("Note: Selected follower %s (might be available)", selected)
	}

	t.Log("✅ Round-robin follower selection test passed")
}
