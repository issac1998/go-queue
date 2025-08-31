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
			continue // Try next broker
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

	// Send controller discovery request (new request type)
	requestType := protocol.ControllerDiscoverRequestType // CONTROLLER_DISCOVERY_REQUEST
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return "", err
	}

	// Read response
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return "", err
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return "", err
	}

	// Parse controller address from response
	return string(responseData), nil
}

// GetControllerAddr returns the current cached controller address
func (c *Client) GetControllerAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.controllerAddr
}

// connect creates a connection to the appropriate broker
// For metadata operations, connects to controller; for data operations, uses partition routing
func (c *Client) connect(forMetadata bool) (net.Conn, error) {
	if forMetadata {
		// For metadata operations, we need to ensure connection to actual controller leader
		return c.connectToController()
	} else {
		// For data operations: use connectToAnyBroker for follower reads
		// This provides load balancing across all brokers for read operations
		return c.connectToAnyBroker()
	}
}

// connectForMetadata creates a connection for metadata operations with read preference
// isWrite: true for write operations (must go to Controller Leader)
// isWrite: false for read operations (can use any broker for follower read)
func (c *Client) connectForMetadata(isWrite bool) (net.Conn, error) {
	if isWrite {
		// Write operations must go to Controller Leader
		return c.connectToController()
	} else {
		// Read operations can use any broker (follower read optimization)
		return c.connectToAnyBroker()
	}
}

// connectToController connects to the actual controller leader with verification
func (c *Client) connectToController() (net.Conn, error) {
	// First try cached controller address
	controllerAddr := c.GetControllerAddr()
	if controllerAddr != "" {
		if conn, err := c.connectAndVerifyController(controllerAddr); err == nil {
			return conn, nil
		}
		// Cached address is invalid, clear it
		c.controllerAddr = ""
	}

	// Discover controller from scratch
	if err := c.DiscoverController(); err != nil {
		return nil, fmt.Errorf("failed to discover controller: %v", err)
	}

	// Try the newly discovered controller
	controllerAddr = c.GetControllerAddr()
	if controllerAddr == "" {
		return nil, fmt.Errorf("no controller address available after discovery")
	}

	return c.connectAndVerifyController(controllerAddr)
}

// connectAndVerifyController connects to a broker and verifies it's the controller leader
func (c *Client) connectAndVerifyController(brokerAddr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", brokerAddr, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", brokerAddr, err)
	}

	// Verify this broker is actually the controller leader
	if err := c.verifyControllerLeader(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("broker %s is not controller leader: %v", brokerAddr, err)
	}

	return conn, nil
}

// verifyControllerLeader sends a verification request to check if the broker is controller leader
func (c *Client) verifyControllerLeader(conn net.Conn) error {
	conn.SetDeadline(time.Now().Add(c.timeout))

	// Send controller verification request
	requestType := protocol.ControllerVerifyRequestType // New request type for verification
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return fmt.Errorf("failed to send verification request: %v", err)
	}

	// Read response
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return fmt.Errorf("failed to read verification response length: %v", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return fmt.Errorf("failed to read verification response: %v", err)
	}

	// Parse response - expect "true" if this broker is controller leader
	isController := string(responseData) == "true"
	if !isController {
		return fmt.Errorf("broker is not controller leader")
	}

	return nil
}

// DEPRECATED: connectToAnyBroker will be removed once all data operations use partition routing
// Use connectForDataOperation(topic, partition, isWrite) instead for data operations
// connectToAnyBroker connects to any available broker (for data operations)
// we will use follwer read after, maybe it will help
func (c *Client) connectToAnyBroker() (net.Conn, error) {
	if len(c.brokerAddrs) == 0 {
		return nil, fmt.Errorf("no broker addresses available")
	}

	// Simplified fallback: just try brokers in order
	// Remove complex load balancing since this should be replaced by partition routing
	for _, brokerAddr := range c.brokerAddrs {
		conn, err := net.DialTimeout("tcp", brokerAddr, c.timeout)
		if err == nil {
			return conn, nil
		}
		// log.Printf("Failed to connect to broker %s: %v", brokerAddr, err) // Removed log.Printf
	}

	return nil, fmt.Errorf("failed to connect to any broker")
}

// sendRequest sends request and handles response
// Now supports specifying whether this is a metadata operation
func (c *Client) sendRequest(requestType int32, requestData []byte) ([]byte, error) {
	return c.sendRequestWithType(requestType, requestData, c.isMetadataRequest(requestType))
}

// sendRequestWithType sends request with explicit metadata flag
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
		// For data operations, use partition-aware routing when possible
		conn, err = c.connect(false)
	}

	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.timeout))

	// Send request type
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to send request data: %v", err)
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}

	// Calculate actual data length based on protocol
	actualDataLen := responseLen

	// Read response data
	responseData := make([]byte, actualDataLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return responseData, nil
}

// isMetadataRequest determines if a request type is a metadata operation
func (c *Client) isMetadataRequest(requestType int32) bool {
	// Define which request types are metadata operations based on protocol constants
	metadataRequestTypes := map[int32]bool{
		2:    true, // CREATE_TOPIC (CreateTopicRequestType)
		10:   true, // LIST_TOPICS (ListTopicsRequestType)
		11:   true, // DESCRIBE_TOPIC (DescribeTopicRequestType)
		12:   true, // DELETE_TOPIC (DeleteTopicRequestType)
		13:   true, // GET_TOPIC_INFO (GetTopicInfoRequestType)
		3:    true, // JOIN_GROUP (JoinGroupRequestType)
		4:    true, // LEAVE_GROUP (LeaveGroupRequestType)
		8:    true, // LIST_GROUPS (ListGroupsRequestType)
		9:    true, // DESCRIBE_GROUP (DescribeGroupRequestType)
		1000: true, // CONTROLLER_DISCOVERY (ControllerDiscoverRequestType)
		1001: true, // CONTROLLER_VERIFY (ControllerVerifyRequestType)
		1002: true, // GET_TOPIC_METADATA (GetTopicMetadataRequestType)
	}

	return metadataRequestTypes[requestType]
}

// isMetadataWriteRequest determines if a request type is a write operation for metadata
func (c *Client) isMetadataWriteRequest(requestType int32) bool {
	// Define which request types are write operations for metadata
	writeRequestTypes := map[int32]bool{
		2:  true, // CREATE_TOPIC (CreateTopicRequestType) - write
		12: true, // DELETE_TOPIC (DeleteTopicRequestType) - write
		3:  true, // JOIN_GROUP (JoinGroupRequestType) - write
		4:  true, // LEAVE_GROUP (LeaveGroupRequestType) - write
		// Read operations (can use follower read):
		// 10: LIST_TOPICS - read
		// 11: DESCRIBE_TOPIC - read
		// 13: GET_TOPIC_INFO - read
		// 8:  LIST_GROUPS - read
		// 9:  DESCRIBE_GROUP - read
		// 1002: GET_TOPIC_METADATA - read
		// Discovery operations (special case, prefer Controller but can fallback):
		1000: false, // CONTROLLER_DISCOVERY - can try any broker first
		1001: false, // CONTROLLER_VERIFY - read operation
	}

	return writeRequestTypes[requestType]
}

// connectForDataOperation connects to the appropriate broker for data operations
// Routes to partition leader for writes, or follower for reads
func (c *Client) connectForDataOperation(topic string, partition int32, isWrite bool) (net.Conn, error) {
	// Get or refresh topic metadata
	metadata, err := c.getTopicMetadata(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %v", err)
	}

	partitionMeta, exists := metadata.Partitions[partition]
	if !exists {
		return nil, fmt.Errorf("partition %d not found for topic %s", partition, topic)
	}

	// Choose target broker based on operation type
	var targetBroker string
	if isWrite {
		// For writes, always go to partition leader
		targetBroker = partitionMeta.Leader
	} else {
		// For reads, implement intelligent follower read selection
		targetBroker = c.selectBrokerForRead(partitionMeta)
	}

	// Connect to the specific broker
	return c.connectToSpecificBroker(targetBroker)
}

// connectToSpecificBroker connects to a specific broker address
func (c *Client) connectToSpecificBroker(brokerAddr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", brokerAddr, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %v", brokerAddr, err)
	}

	return conn, nil
}

// selectBrokerForRead implements intelligent broker selection for read operations
func (c *Client) selectBrokerForRead(partitionMeta PartitionMetadata) string {
	candidates := make([]string, 0, len(partitionMeta.Replicas)+1)

	for _, replica := range partitionMeta.Replicas {
		if replica != partitionMeta.Leader { // Exclude leader from replicas list
			candidates = append(candidates, replica)
		}
	}

	if len(candidates) == 0 {
		return partitionMeta.Leader
	}

	if len(candidates) > 1 {
		selectedBroker := c.selectFollower(candidates[:len(candidates)-1])
		if selectedBroker != "" {
			log.Printf("Selected follower for read: %s", selectedBroker)
			return selectedBroker
		}
	}

	log.Printf("Fallback to leader for read: %s", partitionMeta.Leader)
	return partitionMeta.Leader
}

// selectFollower selects a follower
func (c *Client) selectFollower(followers []string) string {
	if len(followers) == 0 {
		return ""
	}

	// Random Select
	selectedIndex := int(time.Now().UnixNano()) % len(followers)
	selectedFollower := followers[selectedIndex]

	// Basic availability check: try to connect quickly
	if c.isFollowerAvailable(selectedFollower) {
		return selectedFollower
	}

	for i, follower := range followers {
		if i != selectedIndex && c.isFollowerAvailable(follower) {
			return follower
		}
	}

	// If all followers are unavailable, return empty (will fallback to leader)
	return ""
}

// isFollowerAvailable performs a quick availability check for a follower
func (c *Client) isFollowerAvailable(brokerAddr string) bool {
	conn, err := net.DialTimeout("tcp", brokerAddr, 100*time.Millisecond)
	if err != nil {
		log.Printf("Follower %s not available: %v", brokerAddr, err)
		return false
	}
	conn.Close()
	return true
}

// getTopicMetadata retrieves or refreshes topic metadata from controller
func (c *Client) getTopicMetadata(topic string) (*TopicMetadata, error) {
	c.mu.RLock()
	// Check if we have cached metadata that's still valid
	if metadata, exists := c.topicMetadata[topic]; exists {
		if time.Now().Before(metadata.RefreshTime.Add(c.metadataTTL)) {
			c.mu.RUnlock()
			return metadata, nil
		}
		delete(c.topicMetadata, topic)
	}
	c.mu.RUnlock()

	// Need to refresh metadata from controller
	return c.refreshTopicMetadata(topic)
}

// refreshTopicMetadata fetches fresh topic metadata from controller
func (c *Client) refreshTopicMetadata(topic string) (*TopicMetadata, error) {
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		metadata, err := c.tryRefreshTopicMetadata(topic)
		if err == nil {
			// Success - cache the metadata
			c.mu.Lock()
			c.topicMetadata[topic] = metadata
			c.mu.Unlock()
			return metadata, nil
		}

		//stale controller,re-get
		if c.isControllerError(err) {
			log.Printf("Attempt %d: Controller error, rediscovering controller...", attempt+1)

			// Clear cached controller and rediscover
			c.mu.Lock()
			c.controllerAddr = ""
			c.mu.Unlock()

			if discoverErr := c.DiscoverController(); discoverErr != nil {
				log.Printf("Failed to rediscover controller: %v", discoverErr)
			}

			lastErr = fmt.Errorf("controller error, rediscovered for retry: %v", err)
			continue // Retry with new controller
		}

		// For other errors, don't retry
		return nil, err
	}

	return nil, fmt.Errorf("failed to refresh topic metadata after %d attempts, last error: %v", maxRetries, lastErr)
}

// tryRefreshTopicMetadata attempts to fetch topic metadata from the current controller
func (c *Client) tryRefreshTopicMetadata(topic string) (*TopicMetadata, error) {
	// Ensure we have a controller address
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

	// Connect to controller
	conn, err := c.connectToSpecificBroker(controllerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller %s: %v", controllerAddr, err)
	}
	defer conn.Close()

	// Set timeout
	conn.SetDeadline(time.Now().Add(c.timeout))

	// Build request
	requestData, err := c.buildTopicMetadataRequest(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %v", err)
	}

	// Send request type
	requestType := protocol.GetTopicMetadataRequestType
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	// Send request data
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

	// Parse response
	return c.parseTopicMetadataResponse(topic, responseData)
}

// buildTopicMetadataRequest builds the request for topic metadata
func (c *Client) buildTopicMetadataRequest(topic string) ([]byte, error) {
	var buf bytes.Buffer

	// Write topic name length and topic name
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

	// Check for error response first
	var errorCode int32
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return nil, fmt.Errorf("failed to read error code: %v", err)
	}

	if errorCode != 0 {
		// Read error message
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

	// Success response - read partition count
	var partitionCount int32
	if err := binary.Read(buf, binary.BigEndian, &partitionCount); err != nil {
		return nil, fmt.Errorf("failed to read partition count: %v", err)
	}

	// Create metadata structure
	metadata := &TopicMetadata{
		Topic:       topic,
		Partitions:  make(map[int32]PartitionMetadata),
		RefreshTime: time.Now(),
	}

	// Read each partition's metadata
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

		// Read replica count
		var replicaCount int32
		if err := binary.Read(buf, binary.BigEndian, &replicaCount); err != nil {
			return nil, fmt.Errorf("failed to read replica count: %v", err)
		}

		// Read replica addresses
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

		// Store partition metadata
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
