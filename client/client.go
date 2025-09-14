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
	"sync/atomic"
	"time"

	"github.com/issac1998/go-queue/internal/async"
	"github.com/issac1998/go-queue/internal/pool"
	"github.com/issac1998/go-queue/internal/protocol"
)

// Client configuration
type Client struct {
	brokerAddrs    []string
	controllerAddr string
	timeout        time.Duration
	mu             sync.RWMutex

	connectionPool *pool.ConnectionPool
	asyncIO        *async.AsyncIO

	asyncConnections map[string]*async.AsyncConnection
	asyncConnMutex   sync.RWMutex

	config ClientConfig

	topicMetadata map[string]*TopicMetadata
	metadataTTL   time.Duration

	requestIDCounter uint64

	pendingRequests map[uint64]*PendingRequest
	pendingMutex    sync.RWMutex
}

var metadataRequestTypes = map[int32]bool{
	protocol.CreateTopicRequestType:   true,
	protocol.ListTopicsRequestType:    true,
	protocol.DeleteTopicRequestType:   true,
	protocol.GetTopicInfoRequestType:  true,
	protocol.JoinGroupRequestType:     true,
	protocol.LeaveGroupRequestType:    true,
	protocol.ListGroupsRequestType:    true,
	protocol.DescribeGroupRequestType: true,

	protocol.ControllerDiscoverRequestType: true,
	protocol.ControllerVerifyRequestType:   true,
	protocol.GetTopicMetadataRequestType:   true,
	protocol.FetchAssignmentRequestType:    false, // Read operation
	protocol.ProduceRequestType:            false,
	protocol.FetchRequestType:              false,
	protocol.HeartbeatRequestType:          false,
	protocol.CommitOffsetRequestType:       false,
	protocol.FetchOffsetRequestType:        false,
}

var writeRequestTypes = map[int32]bool{
	protocol.CreateTopicRequestType:        true,
	protocol.DeleteTopicRequestType:        true,
	protocol.JoinGroupRequestType:          true,
	protocol.LeaveGroupRequestType:         true,
	protocol.ListTopicsRequestType:         false, // Read operation
	protocol.GetTopicMetadataRequestType:   false, // Read operation
	protocol.GetTopicInfoRequestType:       false, // Read operation
	protocol.ListGroupsRequestType:         false, // Read operation
	protocol.DescribeGroupRequestType:      false, // Read operation
	protocol.ControllerDiscoverRequestType: false, // Read operation
	protocol.ControllerVerifyRequestType:   false, // Read operation
	protocol.FetchAssignmentRequestType:    false, // Read operation
	protocol.ProduceRequestType:            false, // Data operation
	protocol.FetchRequestType:              false, // Data operation
	protocol.HeartbeatRequestType:          false, // Read operation
	protocol.CommitOffsetRequestType:       false, // Data operation
	protocol.FetchOffsetRequestType:        false, // Read operation
}

// TopicMetadata holds partition-to-broker mapping for a topic
type TopicMetadata struct {
	Topic       string
	Partitions  map[int32]PartitionMetadata
	RefreshTime time.Time
}

// PartitionMetadata holds broker information for a specific partition
type PartitionMetadata struct {
	Leader   string
	Replicas []string
}

// ClientConfig client configuration
type ClientConfig struct {
	BrokerAddrs []string
	Timeout     time.Duration

	EnableConnectionPool bool
	EnableAsyncIO        bool

	ConnectionPool pool.ConnectionPoolConfig
	AsyncIO        async.AsyncIOConfig

	BatchSize       int
	BatchTimeout    time.Duration
	MaxPendingBatch int
}

// NewClient creates a new message queue client
func NewClient(config ClientConfig) *Client {
	brokerAddrs := config.BrokerAddrs
	if len(brokerAddrs) == 0 {
		brokerAddrs = []string{"localhost:9092"}
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.BatchTimeout <= 0 {
		config.BatchTimeout = 10 * time.Millisecond
	}
	if config.MaxPendingBatch <= 0 {
		config.MaxPendingBatch = 1000
	}

	client := &Client{
		brokerAddrs:      brokerAddrs,
		timeout:          timeout,
		config:           config,
		topicMetadata:    make(map[string]*TopicMetadata),
		metadataTTL:      5 * time.Minute,
		asyncConnections: make(map[string]*async.AsyncConnection),
		pendingRequests:  make(map[uint64]*PendingRequest),
	}

	if config.EnableConnectionPool {
		client.connectionPool = pool.NewConnectionPool(config.ConnectionPool)
	}

	if client.config.EnableAsyncIO {
		client.asyncIO = async.NewAsyncIO(client.config.AsyncIO)
		if err := client.asyncIO.Start(); err != nil {
			log.Printf("Failed to start AsyncIO: %v", err)
		}
		// Start cleanup routine for pending requests
		client.startCleanupRoutine()
	}
	return client
}

// DiscoverController discovers and caches the current controller leader
func (c *Client) DiscoverController() error {
	var discoveredAddr string

	for _, brokerAddr := range c.brokerAddrs {
		controllerAddr, err := c.queryControllerFromBroker(brokerAddr)
		if err != nil {
			continue
		}
		if controllerAddr != "" {
			discoveredAddr = controllerAddr
			break
		}
	}

	if discoveredAddr == "" {
		return fmt.Errorf("failed to discover controller leader from any broker,brokers:%v", c.brokerAddrs)
	}

	c.setControllerAddr(discoveredAddr)
	return nil
}

// queryControllerFromBroker queries a specific broker for controller information
func (c *Client) queryControllerFromBroker(brokerAddr string) (string, error) {
	log.Printf("Querying controller from broker: %s", brokerAddr)
	conn, err := net.DialTimeout("tcp", brokerAddr, c.timeout)
	if err != nil {
		log.Printf("Failed to connect to broker %s: %v", brokerAddr, err)
		return "", err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.timeout))

	requestType := protocol.ControllerDiscoverRequestType
	log.Printf("Sending controller discover request type: %d", requestType)
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		log.Printf("Failed to send request type: %v", err)
		return "", err
	}

	// Send empty data length for protocol consistency
	dataLength := int32(0)
	if err := binary.Write(conn, binary.BigEndian, dataLength); err != nil {
		log.Printf("Failed to send data length: %v", err)
		return "", err
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		log.Printf("Failed to read response length: %v", err)
		return "", err
	}
	log.Printf("Response length: %d", responseLen)

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		log.Printf("Failed to read response data: %v", err)
		return "", err
	}

	controllerAddr := string(responseData)
	log.Printf("Received controller address: %s", controllerAddr)
	return controllerAddr, nil
}

// GetControllerAddr returns the current cached controller address,reduce consumtion
func (c *Client) GetControllerAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.controllerAddr
}

// setControllerAddr sets the controller address with proper locking
func (c *Client) setControllerAddr(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.controllerAddr = addr
}

// sendMetaRequest sends request and handles response
func (c *Client) sendMetaRequest(requestType int32, requestData []byte) ([]byte, error) {
	log.Printf("Sending meta request type: %d, data length: %d", requestType, len(requestData))
	var conn net.Conn
	var err error

	isWrite := c.isMetadataWriteRequest(requestType)
	log.Printf("Request is write operation: %v", isWrite)
	conn, err = c.connectForMetadata(isWrite)

	if err != nil {
		log.Printf("Failed to connect for metadata: %v", err)
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(c.timeout))

	log.Printf("Sending request type: %d", requestType)
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		log.Printf("Failed to send request type: %v", err)
		return nil, fmt.Errorf("failed to send request type: %v", err)
	}

	// Send request data length
	dataLength := int32(len(requestData))
	if err := binary.Write(conn, binary.BigEndian, dataLength); err != nil {
		log.Printf("Failed to send request data length: %v", err)
		return nil, fmt.Errorf("failed to send request data length: %v", err)
	}
	log.Printf("Sent request data length: %d", dataLength)

	log.Printf("Sending request data of length: %d", len(requestData))
	if _, err := conn.Write(requestData); err != nil {
		log.Printf("Failed to send request data: %v", err)
		return nil, fmt.Errorf("failed to send request data: %v", err)
	}

	log.Printf("Reading response length...")
	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		log.Printf("Failed to read response length: %v", err)
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}
	log.Printf("Response length: %d", responseLen)

	actualDataLen := responseLen

	responseData := make([]byte, actualDataLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		log.Printf("Failed to read response data: %v", err)
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return responseData, nil
}

// isMetadataWriteRequest checks if a metadata request type is a write operation
func (c *Client) isMetadataWriteRequest(requestType int32) bool {
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

	conn, err := protocol.ConnectToSpecificBroker(controllerAddr, c.timeout)
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

	// Send request data length
	dataLength := int32(len(requestData))
	if err := binary.Write(conn, binary.BigEndian, dataLength); err != nil {
		return nil, fmt.Errorf("failed to send request data length: %v", err)
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

// Close close client
func (c *Client) Close() error {
	c.closeAllAsyncConnections()

	if c.connectionPool != nil {
		c.connectionPool.Close()
	}

	if c.asyncIO != nil {
		c.asyncIO.Close()
	}

	return nil
}

// ClientStats client stats
type ClientStats struct {
	ConnectionPool pool.Stats       `json:"connection_pool,omitempty"`
	AsyncIO        async.AsyncStats `json:"async_io,omitempty"`
	TopicCount     int              `json:"topic_count"`
	MetadataTTL    time.Duration    `json:"metadata_ttl"`
}

// GetStats get client stats
func (c *Client) GetStats() ClientStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := ClientStats{
		TopicCount:  len(c.topicMetadata),
		MetadataTTL: c.metadataTTL,
	}

	if c.connectionPool != nil {
		stats.ConnectionPool = c.connectionPool.GetStats()
	}

	if c.asyncIO != nil {
		stats.AsyncIO = c.asyncIO.GetStats()
	}

	return stats
}

func (c *Client) getAsyncConnection(brokerAddr string) (*async.AsyncConnection, error) {
	c.asyncConnMutex.RLock()
	if asyncConn, exists := c.asyncConnections[brokerAddr]; exists {
		c.asyncConnMutex.RUnlock()
		return asyncConn, nil
	}
	c.asyncConnMutex.RUnlock()

	c.asyncConnMutex.Lock()
	defer c.asyncConnMutex.Unlock()

	if asyncConn, exists := c.asyncConnections[brokerAddr]; exists {
		return asyncConn, nil
	}

	conn, err := protocol.ConnectToSpecificBroker(brokerAddr, c.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create async connection to %s: %v", brokerAddr, err)
	}
	// create new one
	asyncConn := c.asyncIO.AddConnection(conn)
	c.asyncConnections[brokerAddr] = asyncConn

	return asyncConn, nil
}

func (c *Client) closeAsyncConnection(brokerAddr string) {
	c.asyncConnMutex.Lock()
	defer c.asyncConnMutex.Unlock()

	if asyncConn, exists := c.asyncConnections[brokerAddr]; exists {
		asyncConn.Close()
		delete(c.asyncConnections, brokerAddr)
	}
}

func (c *Client) closeAllAsyncConnections() {
	c.asyncConnMutex.Lock()
	defer c.asyncConnMutex.Unlock()

	for brokerAddr, asyncConn := range c.asyncConnections {
		asyncConn.Close()
		delete(c.asyncConnections, brokerAddr)
	}
}

// AsyncRequestResult represents the result of an async request
type AsyncRequestResult struct {
	Data []byte
	Err  error
}

// AsyncRequestHandler is a function type for parsing response data
type AsyncRequestHandler func([]byte) (interface{}, error)

// AsyncCallback defines callback function for async operations
type AsyncCallback func(result interface{}, err error)

// PendingRequest represents a pending async request
type PendingRequest struct {
	RequestID  uint64
	BrokerAddr string
	Handler    AsyncRequestHandler
	Callback   AsyncCallback
	CreatedAt  time.Time
	RetryCount int
	MaxRetries int
}

// addPendingRequest adds a pending request to the map
func (c *Client) addPendingRequest(req *PendingRequest) {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()
	c.pendingRequests[req.RequestID] = req
}

// getPendingRequest retrieves a pending request by ID
func (c *Client) getPendingRequest(requestID uint64) (*PendingRequest, bool) {
	c.pendingMutex.RLock()
	defer c.pendingMutex.RUnlock()
	req, exists := c.pendingRequests[requestID]
	return req, exists
}

// removePendingRequest removes a pending request from the map
func (c *Client) removePendingRequest(requestID uint64) {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()
	delete(c.pendingRequests, requestID)
}

// handleAsyncStreamResponse handles async stream response for pending requests
func (c *Client) handleAsyncStreamResponse(userData uint64, conn net.Conn, err error) {
	pendingReq, exists := c.getPendingRequest(userData)
	if !exists {
		log.Printf("No pending request found for userData %d, ignoring response", userData)
		return
	}

	c.removePendingRequest(userData)

	if err != nil {
		pendingReq.Callback(nil, fmt.Errorf("failed to read stream response: %v", err))
		return
	}

	if conn == nil {
		pendingReq.Callback(nil, fmt.Errorf("connection is nil"))
		return
	}

	lengthBuffer := make([]byte, 4)
	n, readErr := io.ReadFull(conn, lengthBuffer)
	if readErr != nil {
		pendingReq.Callback(nil, fmt.Errorf("failed to read response length: %v", readErr))
		return
	}
	if n != 4 {
		pendingReq.Callback(nil, fmt.Errorf("incomplete length header: got %d bytes, expected 4", n))
		return
	}

	responseLen := binary.BigEndian.Uint32(lengthBuffer)
	if responseLen > 100*1024*1024 { // 100MB limit for safety
		pendingReq.Callback(nil, fmt.Errorf("response too large: %d bytes", responseLen))
		return
	}

	responseData := make([]byte, responseLen)
	n, readErr = io.ReadFull(conn, responseData)
	if readErr != nil {
		pendingReq.Callback(nil, fmt.Errorf("failed to read response data: %v", readErr))
		return
	}
	if n != int(responseLen) {
		pendingReq.Callback(nil, fmt.Errorf("incomplete response data: got %d bytes, expected %d", n, responseLen))
		return
	}

	// Process response using handler
	result, parseErr := pendingReq.Handler(responseData)
	if parseErr != nil {
		pendingReq.Callback(nil, fmt.Errorf("failed to parse response: %v", parseErr))
		return
	}

	pendingReq.Callback(result, nil)
}

func (c *Client) cleanupExpiredRequests() {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()

	now := time.Now()
	for requestID, req := range c.pendingRequests {
		if now.Sub(req.CreatedAt) > 30*time.Second { // 30 second timeout
			log.Printf("Request %d expired, removing from pending requests", requestID)
			delete(c.pendingRequests, requestID)
			// Notify callback about timeout
			go req.Callback(nil, fmt.Errorf("request timeout after 30 seconds"))
		}
	}
}

func (c *Client) startCleanupRoutine() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.cleanupExpiredRequests()
			}
		}
	}()
}

// AsyncRequestWithCallback sends request asynchronously and handles response via callback
// This provides async IO with response handling
func (c *Client) AsyncRequestWithCallback(brokerAddr string, requestType int32, requestData []byte, handler AsyncRequestHandler, callback AsyncCallback) error {
	asyncConn, err := c.getAsyncConnection(brokerAddr)
	if err != nil {
		return fmt.Errorf("failed to get async connection: %v", err)
	}

	fullRequest := make([]byte, 4+4+len(requestData))
	binary.BigEndian.PutUint32(fullRequest[:4], uint32(requestType))
	binary.BigEndian.PutUint32(fullRequest[4:8], uint32(len(requestData)))
	copy(fullRequest[8:], requestData)

	counter := atomic.AddUint64(&c.requestIDCounter, 1)
	requestID := uint64(time.Now().Unix())<<32 | (counter & 0xFFFFFFFF)

	pendingReq := &PendingRequest{
		RequestID:  requestID,
		BrokerAddr: brokerAddr,
		Handler:    handler,
		Callback:   callback,
		CreatedAt:  time.Now(),
		MaxRetries: 3,
	}
	c.addPendingRequest(pendingReq)

	return asyncConn.WriteAsync(fullRequest, requestID, func(userData uint64, conn net.Conn, n int, err error) {
		if err != nil {
			log.Printf("Async write failed for %s: %v", brokerAddr, err)
			c.closeAsyncConnection(brokerAddr)
			c.removePendingRequest(userData)
			callback(nil, fmt.Errorf("async write failed: %v", err))
			return
		}
		// write less than respect
		if n < len(fullRequest) {
			c.removePendingRequest(userData)
			callback(nil, fmt.Errorf("incomplete request sent: got %d bytes, expected %d", n, len(fullRequest)))
			return
		}

		c.handleAsyncStreamResponse(userData, conn, nil)
	})
}
