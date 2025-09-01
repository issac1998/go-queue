package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// Client configuration
type Client struct {
	brokerAddrs    []string
	controllerAddr string
	timeout        time.Duration
	mu             sync.RWMutex

	// Partition routing metadata cache
	topicMetadata map[string]*TopicMetadata // Topic → partition mapping
	metadataTTL   time.Duration             // Metadata cache TTL
}

var metadataRequestTypes = map[int32]bool{
	protocol.CreateTopicRequestType:        true, // CREATE_TOPIC (CreateTopicRequestType)
	protocol.ListTopicsRequestType:         true, // LIST_TOPICS (ListTopicsRequestType)
	protocol.DescribeTopicRequestType:      true, // DESCRIBE_TOPIC (DescribeTopicRequestType)
	protocol.DeleteTopicRequestType:        true, // DELETE_TOPIC (DeleteTopicRequestType)
	protocol.GetTopicInfoRequestType:       true, // GET_TOPIC_INFO (GetTopicInfoRequestType)
	protocol.JoinGroupRequestType:          true, // JOIN_GROUP (JoinGroupRequestType)
	protocol.LeaveGroupRequestType:         true, // LEAVE_GROUP (LeaveGroupRequestType)
	protocol.ListGroupsRequestType:         true, // LIST_GROUPS (ListGroupsRequestType)
	protocol.DescribeGroupRequestType:      true, // DESCRIBE_GROUP (DescribeGroupRequestType)
	protocol.ControllerDiscoverRequestType: true, // CONTROLLER_DISCOVERY (ControllerDiscoverRequestType)
	protocol.ControllerVerifyRequestType:   true, // CONTROLLER_VERIFY (ControllerVerifyRequestType)
	protocol.GetTopicMetadataRequestType:   true, // GET_TOPIC_METADATA (GetTopicMetadataRequestType)
}

var writeRequestTypes = map[int32]bool{
	protocol.CreateTopicRequestType: true, // CREATE_TOPIC (CreateTopicRequestType) - write
	protocol.DeleteTopicRequestType: true, // DELETE_TOPIC (DeleteTopicRequestType) - write
	protocol.JoinGroupRequestType:   true, // JOIN_GROUP (JoinGroupRequestType) - write
	protocol.LeaveGroupRequestType:  true, // LEAVE_GROUP (LeaveGroupRequestType) - write
}

// TopicMetadata holds partition-to-broker mapping for a topic
type TopicMetadata struct {
	Topic       string
	Partitions  map[int32]PartitionMetadata // partition_id → broker info
	RefreshTime time.Time
}

// PartitionMetadata holds broker information for a specific partition
type PartitionMetadata struct {
	Leader   string   // Leader broker address
	Replicas []string // Replica broker addresses (for follower reads)
}

// ClientConfig client configuration
type ClientConfig struct {
	BrokerAddrs []string
	Timeout     time.Duration
}

// NewClient creates a new message queue client
func NewClient(config ClientConfig) *Client {
	return &Client{
		brokerAddrs:   config.BrokerAddrs,
		timeout:       time.Duration(config.Timeout) * time.Millisecond,
		topicMetadata: make(map[string]*TopicMetadata),
		metadataTTL:   5 * time.Minute, // Cache metadata for 5 minutes
	}
}

// DiscoverController discovers and caches the current controller leader
func (c *Client) DiscoverController() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, brokerAddr := range c.brokerAddrs {
		controllerAddr, err := c.queryControllerFromBroker(brokerAddr)
		if err != nil {
			continue
		}

		if controllerAddr != "" {
			c.controllerAddr = controllerAddr
			return nil
		}
	}

	return fmt.Errorf("failed to discover controller leader from any broker")
}

// queryControllerFromBroker queries a specific broker for controller information
func (c *Client) queryControllerFromBroker(brokerAddr string) (string, error) {
	conn, err := net.DialTimeout("tcp", brokerAddr, c.timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.timeout))

	requestType := protocol.ControllerDiscoverRequestType
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return "", err
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return "", err
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return "", err
	}

	return string(responseData), nil
}

// GetControllerAddr returns the current cached controller address,reduce consumtion
func (c *Client) GetControllerAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.controllerAddr
}

// sendRequest sends request and handles response
func (c *Client) sendRequest(requestType int32, requestData []byte) ([]byte, error) {
	return c.sendRequestWithType(requestType, requestData, c.isMetadataRequest(requestType))
}

// sendRequestWithType sends request with explicit metadata flag
// TODO :Data Operation,use this method or connectForDataOperation？
func (c *Client) sendRequestWithType(requestType int32, requestData []byte, isMetadata bool) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var conn net.Conn
	var err error

	if isMetadata {
		// For metadata operations, determine if it's read or write
		isWrite := c.isMetadataWriteRequest(requestType)
		conn, err = c.connectForMetadata(isWrite)
	} else {
		// TODO :Data Operation,use this method or connectForDataOperation？
		// conn, err = c.connectForDataOperation(requestData, false)
	}

	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.timeout))

	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to send request data: %v", err)
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}

	actualDataLen := responseLen

	responseData := make([]byte, actualDataLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return responseData, nil
}

// isMetadataRequest determines if a request type is a metadata operation
func (c *Client) isMetadataRequest(requestType int32) bool {
	return metadataRequestTypes[requestType]
}

// isMetadataWriteRequest determines if a request type is a write operation for metadata
func (c *Client) isMetadataWriteRequest(requestType int32) bool {
	// Define which request types are write operations for metadata
	return writeRequestTypes[requestType]
}

// getTopicMetadata retrieves or refreshes topic metadata from controller
// TODO: How we use cache?with ttl or just simply get cache,use it until following request return false?
// There's a bug now that if a metadata is stale, no one is going to refresh it untine
func (c *Client) getTopicMetadata(topic string) (*TopicMetadata, error) {
	c.mu.RLock()
	if metadata, exists := c.topicMetadata[topic]; exists {
		if time.Now().Before(metadata.RefreshTime.Add(c.metadataTTL)) {
			c.mu.RUnlock()
			return metadata, nil
		}
		delete(c.topicMetadata, topic)
	}
	c.mu.RUnlock()

	return c.refreshTopicMetadata(topic)
}

// refreshTopicMetadata fetches fresh topic metadata from controller
func (c *Client) refreshTopicMetadata(topic string) (*TopicMetadata, error) {
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		metadata, err := c.tryRefreshTopicMetadata(topic)
		if err == nil {
			c.mu.Lock()
			c.topicMetadata[topic] = metadata
			c.mu.Unlock()
			return metadata, nil
		}

		//stale controller,re-get
		if c.isControllerError(err) {
			log.Printf("Attempt %d: Controller error, rediscovering controller...", attempt+1)

			c.mu.Lock()
			c.controllerAddr = ""
			c.mu.Unlock()

			if discoverErr := c.DiscoverController(); discoverErr != nil {
				log.Printf("Failed to rediscover controller: %v", discoverErr)
			}

			lastErr = fmt.Errorf("controller error, rediscovered for retry: %v", err)
			continue // Retry with new controller
		}

		return nil, err
	}

	return nil, fmt.Errorf("failed to refresh topic metadata after %d attempts, last error: %v", maxRetries, lastErr)
}

// tryRefreshTopicMetadata attempts to fetch topic metadata from the current controller
func (c *Client) tryRefreshTopicMetadata(topic string) (*TopicMetadata, error) {
	// can be stale, I got retry outside
	controllerAddr := c.GetControllerAddr()
	if controllerAddr == "" {
		if err := c.DiscoverController(); err != nil {
			return nil, fmt.Errorf("failed to discover controller: %v", err)
		}
		controllerAddr = c.GetControllerAddr()
		if controllerAddr == "" {
			return nil, fmt.Errorf("no controller available")
		}
	}

	conn, err := c.connectToSpecificBroker(controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller %s: %v", controllerAddr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.timeout))

	requestData, err := c.buildTopicMetadataRequest(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	requestType := protocol.GetTopicMetadataRequestType
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to send request data: %v", err)
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return c.parseTopicMetadataResponse(topic, responseData)
}

// buildTopicMetadataRequest builds the request for topic metadata
func (c *Client) buildTopicMetadataRequest(topic string) ([]byte, error) {
	var buf bytes.Buffer

	topicBytes := []byte(topic)
	topicLen := int32(len(topicBytes))

	if err := binary.Write(&buf, binary.BigEndian, topicLen); err != nil {
		return nil, err
	}

	if _, err := buf.Write(topicBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// parseTopicMetadataResponse parses the topic metadata response
func (c *Client) parseTopicMetadataResponse(topic string, responseData []byte) (*TopicMetadata, error) {
	buf := bytes.NewReader(responseData)

	var errorCode int32
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		var errorMsgLen int32
		if err := binary.Read(buf, binary.BigEndian, &errorMsgLen); err != nil {
			return nil, fmt.Errorf("failed to read error message length: %v", err)
		}

		errorMsgBytes := make([]byte, errorMsgLen)
		if _, err := io.ReadFull(buf, errorMsgBytes); err != nil {
			return nil, fmt.Errorf("failed to read error message: %v", err)
		}

		return nil, fmt.Errorf("server error %d: %s", errorCode, string(errorMsgBytes))
	}

	var partitionCount int32
	if err := binary.Read(buf, binary.BigEndian, &partitionCount); err != nil {
		return nil, fmt.Errorf("failed to read partition count: %v", err)
	}

	metadata := &TopicMetadata{
		Topic:       topic,
		Partitions:  make(map[int32]PartitionMetadata),
		RefreshTime: time.Now(),
	}

	for i := int32(0); i < partitionCount; i++ {
		var partitionID int32
		if err := binary.Read(buf, binary.BigEndian, &partitionID); err != nil {
			return nil, fmt.Errorf("failed to read partition ID: %v", err)
		}

		// Read leader address
		var leaderAddrLen int32
		if err := binary.Read(buf, binary.BigEndian, &leaderAddrLen); err != nil {
			return nil, fmt.Errorf("failed to read leader address length: %v", err)
		}

		leaderAddrBytes := make([]byte, leaderAddrLen)
		if _, err := io.ReadFull(buf, leaderAddrBytes); err != nil {
			return nil, fmt.Errorf("failed to read leader address: %v", err)
		}

		var replicaCount int32
		if err := binary.Read(buf, binary.BigEndian, &replicaCount); err != nil {
			return nil, fmt.Errorf("failed to read replica count: %v", err)
		}

		replicas := make([]string, replicaCount)
		for j := int32(0); j < replicaCount; j++ {
			var replicaAddrLen int32
			if err := binary.Read(buf, binary.BigEndian, &replicaAddrLen); err != nil {
				return nil, fmt.Errorf("failed to read replica address length: %v", err)
			}

			replicaAddrBytes := make([]byte, replicaAddrLen)
			if _, err := io.ReadFull(buf, replicaAddrBytes); err != nil {
				return nil, fmt.Errorf("failed to read replica address: %v", err)
			}

			replicas[j] = string(replicaAddrBytes)
		}

		metadata.Partitions[partitionID] = PartitionMetadata{
			Leader:   string(leaderAddrBytes),
			Replicas: replicas,
		}
	}

	return metadata, nil
}

// isControllerError checks if the error indicates a controller-related issue
func (c *Client) isControllerError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "controller") ||
		strings.Contains(errStr, "not leader") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset")
}
