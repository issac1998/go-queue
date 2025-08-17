package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Admin management client
type Admin struct {
	client *Client
}

// NewAdmin creates a new admin client
func NewAdmin(client *Client) *Admin {
	return &Admin{
		client: client,
	}
}

// CreateTopicRequest create topic request
type CreateTopicRequest struct {
	Name       string
	Partitions int32
	Replicas   int32 // Not supported yet, set to 1
}

// CreateTopicResult create topic result
type CreateTopicResult struct {
	Name  string
	Error error
}

// CreateTopic creates a topic
func (a *Admin) CreateTopic(req CreateTopicRequest) (*CreateTopicResult, error) {
	// Set default values
	if req.Partitions <= 0 {
		req.Partitions = 1
	}
	if req.Replicas <= 0 {
		req.Replicas = 1
	}

	// Build request data
	requestData, err := a.buildCreateTopicRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	// Send request
	responseData, err := a.client.sendRequest(CreateTopicRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	// Parse response
	result, err := a.parseCreateTopicResponse(req.Name, responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result, nil
}

// buildCreateTopicRequest builds create topic request
func (a *Admin) buildCreateTopicRequest(req CreateTopicRequest) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. Protocol version
	if err := binary.Write(buf, binary.BigEndian, int16(ProtocolVersion)); err != nil {
		return nil, err
	}

	// 2. Topic name length and content
	if err := binary.Write(buf, binary.BigEndian, int16(len(req.Name))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(req.Name); err != nil {
		return nil, err
	}

	// 3. Number of partitions
	if err := binary.Write(buf, binary.BigEndian, req.Partitions); err != nil {
		return nil, err
	}

	// 4. Number of replicas (not supported yet, but kept for protocol completeness)
	if err := binary.Write(buf, binary.BigEndian, req.Replicas); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// parseCreateTopicResponse parses create topic response
func (a *Admin) parseCreateTopicResponse(topicName string, data []byte) (*CreateTopicResult, error) {
	buf := bytes.NewReader(data)

	result := &CreateTopicResult{
		Name: topicName,
	}

	// Read error code
	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	// Check error code
	if errorCode != 0 {
		result.Error = fmt.Errorf("failed to create topic, error code: %d", errorCode)
	}

	return result, nil
}

// TopicInfo topic information
type TopicInfo struct {
	Name       string
	Partitions []PartitionInfo
}

// PartitionInfo partition information
type PartitionInfo struct {
	ID       int32
	Leader   int32   // Not supported yet
	Replicas []int32 // Not supported yet
}

// ListTopics lists all topics (not implemented yet, requires server support)
func (a *Admin) ListTopics() ([]TopicInfo, error) {
	// This feature requires server-side protocol support
	return nil, fmt.Errorf("ListTopics feature not implemented yet, requires server support")
}

// DeleteTopic deletes a topic (not implemented yet, requires server support)
func (a *Admin) DeleteTopic(name string) error {
	// This feature requires server-side protocol support
	return fmt.Errorf("DeleteTopic feature not implemented yet, requires server support")
}

// GetTopicInfo gets topic information (not implemented yet, requires server support)
func (a *Admin) GetTopicInfo(name string) (*TopicInfo, error) {
	// This feature requires server-side protocol support
	return nil, fmt.Errorf("GetTopicInfo feature not implemented yet, requires server support")
}
