package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
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

// TopicInfo represents topic information
type TopicInfo struct {
	Name             string
	Partitions       int32
	Replicas         int32
	MessageCount     int64
	Size             int64
	CreatedAt        time.Time
	PartitionDetails []PartitionInfo
}

// PartitionInfo represents partition information
type PartitionInfo struct {
	ID           int32
	Leader       int32
	Replicas     []int32
	ISR          []int32
	Size         int64
	MessageCount int64
	StartOffset  int64
	EndOffset    int64
}

// SimpleTopicInfo represents basic topic information
type SimpleTopicInfo struct {
	Name         string
	Partitions   int32
	MessageCount int64
	Size         int64
}

// CreateTopic creates a topic
func (a *Admin) CreateTopic(req CreateTopicRequest) (*CreateTopicResult, error) {
	if req.Partitions <= 0 {
		req.Partitions = 1
	}
	if req.Replicas <= 0 {
		req.Replicas = 1
	}

	requestData, err := a.buildCreateTopicRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	responseData, err := a.client.sendRequest(protocol.CreateTopicRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	result, err := a.parseCreateTopicResponse(req.Name, responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return result, nil
}

// ListTopics lists all topics (for backward compatibility)
func (a *Admin) ListTopics() ([]TopicInfo, error) {
	requestData, err := a.buildListTopicsRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	responseData, err := a.client.sendRequest(protocol.ListTopicsRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	topics, err := a.parseListTopicsResponse(responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return topics, nil
}

// ListTopicsWithFollowerRead lists all topics using follower read optimization
func (a *Admin) ListTopicsWithFollowerRead() ([]TopicInfo, error) {
	requestData, err := a.buildListTopicsRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	// Use follower read optimization for list operations
	responseData, err := a.client.sendRequestWithType(protocol.ListTopicsRequestType, requestData, true)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	topics, err := a.parseListTopicsResponse(responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return topics, nil
}

// ListTopicsFromController explicitly lists topics from controller leader (for consistency)
func (a *Admin) ListTopicsFromController() ([]TopicInfo, error) {
	// Connect directly to controller for consistent reads
	conn, err := a.client.connectToController()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(a.client.timeout))

	// Send request type
	requestType := protocol.ListTopicsRequestType
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	// Build and send request data
	requestData, err := a.buildListTopicsRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to send request data: %v", err)
	}

	// Read response
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return a.parseListTopicsResponse(responseData)
}


// DeleteTopic deletes a topic
func (a *Admin) DeleteTopic(topicName string) error {
	requestData, err := a.buildDeleteTopicRequest(topicName)
	if err != nil {
		return fmt.Errorf("failed to build request: %v", err)
	}

	responseData, err := a.client.sendRequest(protocol.DeleteTopicRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}

	err = a.parseDeleteTopicResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse response: %v", err)
	}

	return nil
}

// GetTopicInfo gets basic topic information
func (a *Admin) GetTopicInfo(topicName string) (*TopicInfo, error) {
	requestData, err := a.buildGetTopicInfoRequest(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	responseData, err := a.client.sendRequest(protocol.GetTopicInfoRequestType, requestData)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	info, err := a.parseGetTopicInfoResponse(responseData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	return info, nil
}

// buildCreateTopicRequest builds create topic request
func (a *Admin) buildCreateTopicRequest(req CreateTopicRequest) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(req.Name))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(req.Name); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, req.Partitions); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, req.Replicas); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// buildListTopicsRequest builds list topics request
func (a *Admin) buildListTopicsRequest() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// buildDescribeTopicRequest builds describe topic request
func (a *Admin) buildDescribeTopicRequest(topicName string) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(topicName))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topicName); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// buildDeleteTopicRequest builds delete topic request
func (a *Admin) buildDeleteTopicRequest(topicName string) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(topicName))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topicName); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// buildGetTopicInfoRequest builds get topic info request
func (a *Admin) buildGetTopicInfoRequest(topicName string) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ProtocolVersion)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(topicName))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topicName); err != nil {
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

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		result.Error = fmt.Errorf("failed to create topic, error code: %d", errorCode)
	}

	return result, nil
}

// parseListTopicsResponse parses list topics response
func (a *Admin) parseListTopicsResponse(data []byte) ([]TopicInfo, error) {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		return nil, fmt.Errorf("server error, code: %d", errorCode)
	}

	var topicCount int32
	if err := binary.Read(buf, binary.BigEndian, &topicCount); err != nil {
		return nil, fmt.Errorf("failed to read topic count: %v", err)
	}

	topics := make([]TopicInfo, topicCount)
	for i := int32(0); i < topicCount; i++ {

		var nameLen int16
		if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
			return nil, fmt.Errorf("failed to read topic name length: %v", err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(buf, nameBytes); err != nil {
			return nil, fmt.Errorf("failed to read topic name: %v", err)
		}
		topics[i].Name = string(nameBytes)

		if err := binary.Read(buf, binary.BigEndian, &topics[i].Partitions); err != nil {
			return nil, fmt.Errorf("failed to read partitions: %v", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &topics[i].Replicas); err != nil {
			return nil, fmt.Errorf("failed to read replicas: %v", err)
		}

		var createdAtUnix int64
		if err := binary.Read(buf, binary.BigEndian, &createdAtUnix); err != nil {
			return nil, fmt.Errorf("failed to read created time: %v", err)
		}
		topics[i].CreatedAt = time.Unix(createdAtUnix, 0)

		if err := binary.Read(buf, binary.BigEndian, &topics[i].Size); err != nil {
			return nil, fmt.Errorf("failed to read size: %v", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &topics[i].MessageCount); err != nil {
			return nil, fmt.Errorf("failed to read message count: %v", err)
		}
	}

	return topics, nil
}

// parseGetTopicInfoResponse parses describe topic response
func (a *Admin) parseGetTopicInfoResponse(data []byte) (*TopicInfo, error) {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		return nil, fmt.Errorf("server error, code: %d", errorCode)
	}

	topic := &TopicInfo{}

	var nameLen int16
	if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
		return nil, fmt.Errorf("failed to read topic name length: %v", err)
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic name: %v", err)
	}
	topic.Name = string(nameBytes)

	if err := binary.Read(buf, binary.BigEndian, &topic.Partitions); err != nil {
		return nil, fmt.Errorf("failed to read partitions: %v", err)
	}
	if err := binary.Read(buf, binary.BigEndian, &topic.Replicas); err != nil {
		return nil, fmt.Errorf("failed to read replicas: %v", err)
	}

	var createdAtUnix int64
	if err := binary.Read(buf, binary.BigEndian, &createdAtUnix); err != nil {
		return nil, fmt.Errorf("failed to read created time: %v", err)
	}
	topic.CreatedAt = time.Unix(createdAtUnix, 0)

	if err := binary.Read(buf, binary.BigEndian, &topic.Size); err != nil {
		return nil, fmt.Errorf("failed to read size: %v", err)
	}
	if err := binary.Read(buf, binary.BigEndian, &topic.MessageCount); err != nil {
		return nil, fmt.Errorf("failed to read message count: %v", err)
	}

	var partitionCount int32
	if err := binary.Read(buf, binary.BigEndian, &partitionCount); err != nil {
		return nil, fmt.Errorf("failed to read partition count: %v", err)
	}

	topic.PartitionDetails = make([]PartitionInfo, partitionCount)
	for i := int32(0); i < partitionCount; i++ {
		partition := &topic.PartitionDetails[i]
		if err := binary.Read(buf, binary.BigEndian, &partition.ID); err != nil {
			return nil, fmt.Errorf("failed to read partition ID: %v", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &partition.Leader); err != nil {
			return nil, fmt.Errorf("failed to read partition leader: %v", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &partition.Size); err != nil {
			return nil, fmt.Errorf("failed to read partition size: %v", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &partition.MessageCount); err != nil {
			return nil, fmt.Errorf("failed to read partition message count: %v", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &partition.StartOffset); err != nil {
			return nil, fmt.Errorf("failed to read partition start offset: %v", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &partition.EndOffset); err != nil {
			return nil, fmt.Errorf("failed to read partition end offset: %v", err)
		}

		partition.Replicas = []int32{0}
		partition.ISR = []int32{0}
	}

	return topic, nil
}

// parseDeleteTopicResponse parses delete topic response
func (a *Admin) parseDeleteTopicResponse(data []byte) error {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		return fmt.Errorf("failed to delete topic, error code: %d", errorCode)
	}

	return nil
}
