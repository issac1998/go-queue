package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"time"

	"github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/raft"
)

// ClientServer handles client connections and requests
type ClientServer struct {
	broker   *Broker
	listener net.Listener
}

// RequestHandler defines the interface for handling specific request types
type RequestHandler interface {
	Handle(conn net.Conn, cs *ClientServer) error
}

// RequestType categorizes different types of requests
type RequestType int

const (
	MetadataWriteRequest RequestType = iota
	MetadataReadRequest
	DataRequest
	ControllerRequest
	InterBrokerRequest
)

// RequestConfig defines configuration for each request type
type RequestConfig struct {
	Type    RequestType
	Handler RequestHandler
}

// requestConfigs maps request types to their configurations
var requestConfigs = map[int32]RequestConfig{
	protocol.ControllerDiscoverRequestType:      {Type: ControllerRequest, Handler: &ControllerDiscoveryHandler{}},
	protocol.ControllerVerifyRequestType:        {Type: ControllerRequest, Handler: &ControllerVerifyHandler{}},
	protocol.CreateTopicRequestType:             {Type: MetadataWriteRequest, Handler: &CreateTopicHandler{}},
	protocol.DeleteTopicRequestType:             {Type: MetadataWriteRequest, Handler: &DeleteTopicHandler{}},
	protocol.ListTopicsRequestType:              {Type: MetadataReadRequest, Handler: &ListTopicsHandler{}},
	protocol.GetTopicInfoRequestType:            {Type: MetadataReadRequest, Handler: &GetTopicInfoHandler{}},
	protocol.JoinGroupRequestType:               {Type: MetadataWriteRequest, Handler: &JoinGroupHandler{}},
	protocol.LeaveGroupRequestType:              {Type: MetadataWriteRequest, Handler: &LeaveGroupHandler{}},
	protocol.ProduceRequestType:                 {Type: DataRequest, Handler: &ProduceHandler{}},
	protocol.FetchRequestType:                   {Type: DataRequest, Handler: &FetchHandler{}},
	protocol.GetTopicMetadataRequestType:        {Type: MetadataReadRequest, Handler: &GetTopicMetadataHandler{}},
	protocol.StartPartitionRaftGroupRequestType: {Type: InterBrokerRequest, Handler: &StartPartitionRaftGroupHandler{}},
	protocol.StopPartitionRaftGroupRequestType:  {Type: InterBrokerRequest, Handler: &StopPartitionRaftGroupHandler{}},
	protocol.HeartbeatRequestType:               {Type: MetadataWriteRequest, Handler: &HeartbeatHandler{}},
	protocol.CommitOffsetRequestType:            {Type: MetadataWriteRequest, Handler: &CommitOffsetHandler{}},
	protocol.FetchOffsetRequestType:             {Type: MetadataReadRequest, Handler: &FetchOffsetHandler{}},
	protocol.FetchAssignmentRequestType:         {Type: MetadataReadRequest, Handler: &FetchAssignmentHandler{}},
	protocol.TransactionPrepareRequestType:      {Type: DataRequest, Handler: &TransactionPrepareHandler{}},
	protocol.TransactionCommitRequestType:       {Type: DataRequest, Handler: &TransactionCommitHandler{}},
	protocol.TransactionRollbackRequestType:     {Type: DataRequest, Handler: &TransactionRollbackHandler{}},
	protocol.OrderedProduceRequestType:          {Type: DataRequest, Handler: &OrderedProduceHandler{}},
}

// NewClientServer creates a new ClientServer
func NewClientServer(broker *Broker) (*ClientServer, error) {
	return &ClientServer{
		broker: broker,
	}, nil
}

// Start starts the client server
func (cs *ClientServer) Start() error {
	addr := fmt.Sprintf("%s:%d", cs.broker.Address, cs.broker.Port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", addr, err)
	}

	cs.listener = listener
	go cs.acceptConnections()

	log.Printf("Client server listening on %s", addr)
	return nil
}

// Stop stops the client server
func (cs *ClientServer) Stop() error {
	if cs.listener != nil {
		return cs.listener.Close()
	}
	return nil
}

// acceptConnections accepts and handles client connections
func (cs *ClientServer) acceptConnections() {
	for {
		conn, err := cs.listener.Accept()
		if err != nil {
			// Server is probably shutting down
			return
		}
		go cs.handleConnection(conn)
	}
}

// handleConnection handles incoming client connections
func (cs *ClientServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	log.Printf("New client connection from %s", conn.RemoteAddr())
	// Set read timeout for reading request type
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	// Read request type
	var requestType int32
	if err := binary.Read(conn, binary.BigEndian, &requestType); err != nil {
		log.Printf("Failed to read request type: %v", err)
		return
	}

	log.Printf("Received request type: %d", requestType)

	// Get request configuration
	config, exists := requestConfigs[requestType]
	if !exists {
		log.Printf("Unknown request type: %d", requestType)
		cs.sendErrorResponse(conn, fmt.Errorf("unknown request type: %d", requestType))
		return
	}

	// Handle the request based on its type
	if err := cs.handleRequestByType(conn, requestType, config); err != nil {
		log.Printf("Failed to handle request type %d: %v", requestType, err)
		cs.sendErrorResponse(conn, err)
	}
}

// handleRequestByType handles requests based on their type and configuration
func (cs *ClientServer) handleRequestByType(conn net.Conn, requestType int32, config RequestConfig) error {
	switch config.Type {
	case ControllerRequest:
		return config.Handler.Handle(conn, cs)
	case MetadataWriteRequest:
		return cs.handleMetadataWriteRequest(conn, config)
	case MetadataReadRequest:
		return cs.handleMetadataReadRequest(conn, config)
	case DataRequest:
		return config.Handler.Handle(conn, cs)

	case InterBrokerRequest:
		return config.Handler.Handle(conn, cs)

	default:
		return fmt.Errorf("unknown request type category")
	}
}

func (cs *ClientServer) isLeader() bool {
	// Check if raftManager is properly initialized
	if cs.broker.raftManager == nil {
		return false
	}

	leaderID, exists, err := cs.broker.raftManager.GetLeaderID(raft.ControllerGroupID)
	if err != nil || !exists {
		return false
	}

	// Check if Controller is properly initialized
	if cs.broker.Controller == nil {
		return false
	}

	if leaderID == cs.broker.Controller.brokerIDToNodeID(cs.broker.ID) {
		return true
	}
	return false
}

// handleMetadataWriteRequest handles requests that can only be processed by Controller Leader
func (cs *ClientServer) handleMetadataWriteRequest(conn net.Conn, config RequestConfig) error {
	leaderID, exists := cs.broker.Controller.GetControlledLeaderID()
	if !exists {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}
	if leaderID != cs.broker.Controller.brokerIDToNodeID(cs.broker.ID) {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: fmt.Sprintf("not controller leader, please redirect to: %s", cs.getControllerLeaderAddr()),
		}
	}

	return config.Handler.Handle(conn, cs)
}

// handleMetadataReadRequest handles metadata read requests with follower read support
func (cs *ClientServer) handleMetadataReadRequest(conn net.Conn, config RequestConfig) error {
	leaderID, exists := cs.broker.Controller.GetControlledLeaderID()
	// leader
	if exists && leaderID == cs.broker.Controller.brokerIDToNodeID(cs.broker.ID) {
		return config.Handler.Handle(conn, cs)
	}
	// follower

	isFollowerReadEnabled := cs.broker.Config.EnableFollowerRead

	if isFollowerReadEnabled {
		if err := cs.ensureReadIndexConsistency(); err != nil {
			log.Printf("ReadIndex failed: %v", err)
			return fmt.Errorf("read consistency check failed: %v", err)
		}
		return config.Handler.Handle(conn, cs)
	}

	return &errors.TypedError{
		Type:    errors.ControllerError,
		Message: "not controller leader, can't do follower read either",
	}
}

// ensureReadIndexConsistency uses Dragonboat's ReadIndex to ensure read consistency
func (cs *ClientServer) ensureReadIndexConsistency() error {
	if cs.broker.Controller == nil {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}
	// wait readindex catch up for 5 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := cs.broker.raftManager.EnsureReadIndexConsistency(ctx, raft.ControllerGroupID)
	if err != nil {
		return fmt.Errorf("failed to ensure read index consistency: %v", err)
	}
	return nil
}

// getControllerLeaderAddr gets the controller leader address
func (cs *ClientServer) getControllerLeaderAddr() string {
	if cs.broker.Controller == nil {
		return ""
	}

	leaderID, exists := cs.broker.Controller.GetControlledLeaderID()
	if !exists {
		return ""
	}

	brokers, err := cs.broker.discovery.DiscoverBrokers()
	if err != nil {
		return ""
	}

	for _, broker := range brokers {
		nodeID := cs.broker.Controller.brokerIDToNodeID(broker.ID)
		if nodeID == leaderID {
			return fmt.Sprintf("%s:%d", broker.Address, broker.Port)
		}
	}

	return ""
}

func (cs *ClientServer) sendErrorResponse(conn net.Conn, err error) {
	// Set write timeout for sending response
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	var buf bytes.Buffer
	errorCode := int16(protocol.ErrorInternalError)
	binary.Write(&buf, binary.BigEndian, errorCode)

	// Add error message
	errorMsg := err.Error()
	errorMsgBytes := []byte(errorMsg)
	binary.Write(&buf, binary.BigEndian, int32(len(errorMsgBytes)))
	buf.Write(errorMsgBytes)

	responseData := buf.Bytes()
	responseLen := int32(len(responseData))

	binary.Write(conn, binary.BigEndian, responseLen)
	conn.Write(responseData)
	log.Printf("Sent error response with error code: %d, message: %s", errorCode, errorMsg)
}

func (cs *ClientServer) sendSuccessResponse(conn net.Conn, data []byte) {
	// Set write timeout for sending response
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))

	responseLen := int32(len(data))
	binary.Write(conn, binary.BigEndian, responseLen)
	conn.Write(data)
}

// readRequestData reads variable-length request data from the connection
// Client sends: RequestType + DataLength + RequestData
// handleConnection already read RequestType, so we read DataLength + RequestData
// Uses the connection timeout set by handleConnection (30 seconds)
func (cs *ClientServer) readRequestData(conn net.Conn) ([]byte, error) {
	// Set read timeout for this operation to avoid i/o timeout issues
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		return nil, fmt.Errorf("failed to read data length: %v", err)
	}

	const maxRequestSize = 10 * 1024 * 1024
	if dataLength < 0 || dataLength > maxRequestSize {
		return nil, fmt.Errorf("invalid data length: %d (max: %d)", dataLength, maxRequestSize)
	}

	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	requestData := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, requestData); err != nil {
		return nil, fmt.Errorf("failed to read request data: %v", err)
	}

	return requestData, nil
}

// ControllerDiscoveryHandler handles controller discovery requests
type ControllerDiscoveryHandler struct{}

func (h *ControllerDiscoveryHandler) Handle(conn net.Conn, cs *ClientServer) error {
	// Read request data to maintain protocol consistency
	_, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	var controllerAddr string

	if cs.isLeader() {
		controllerAddr = fmt.Sprintf("%s:%d", cs.broker.Address, cs.broker.Port)
	} else {
		controllerAddr = cs.getControllerLeaderAddr()
	}

	response := []byte(controllerAddr)
	responseLen := int32(len(response))

	if err := binary.Write(conn, binary.BigEndian, responseLen); err != nil {
		return fmt.Errorf("failed to send response length: %v", err)
	}

	if _, err := conn.Write(response); err != nil {
		return fmt.Errorf("failed to send response: %v", err)
	}

	log.Printf("Controller discovery response: %s", controllerAddr)
	return nil
}

// ControllerVerifyHandler handles controller verification requests
type ControllerVerifyHandler struct{}

func (h *ControllerVerifyHandler) Handle(conn net.Conn, cs *ClientServer) error {
	// Read request data to maintain protocol consistency
	_, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	isLeader := cs.broker.Controller != nil && cs.broker.Controller.isLeader()

	var response []byte
	if isLeader {
		response = []byte("true")
	} else {
		response = []byte("false")
	}

	responseLen := int32(len(response))
	if err := binary.Write(conn, binary.BigEndian, responseLen); err != nil {
		return fmt.Errorf("failed to send response length: %v", err)
	}

	if _, err := conn.Write(response); err != nil {
		return fmt.Errorf("failed to send response: %v", err)
	}

	log.Printf("Controller verify response: %s", string(response))
	return nil
}

// CreateTopicHandler handles create topic requests
type CreateTopicHandler struct{}

func (h *CreateTopicHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	topicName, partitions, replicas, err := h.parseCreateTopicRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	if topicName == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if partitions <= 0 {
		return fmt.Errorf("partitions must be greater than 0")
	}
	if replicas <= 0 {
		return fmt.Errorf("replicas must be greater than 0")
	}

	err = cs.broker.Controller.CreateTopic(topicName, partitions, replicas)
	fmt.Println("CreateTopic result:", err)
	if err != nil {
		log.Printf("Failed to create topic '%s': %v", topicName, err)
		return fmt.Errorf("failed to create topic: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return fmt.Errorf("failed to write success response: %v", err)
	}

	// Add empty error message for consistency with error response format
	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return fmt.Errorf("failed to write error message length: %v", err)
	}

	cs.sendSuccessResponse(conn, buf.Bytes())
	log.Printf("Successfully created topic '%s' with %d partitions", topicName, partitions)
	return nil
}

func (h *CreateTopicHandler) parseCreateTopicRequest(data []byte) (string, int32, int32, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", 0, 0, err
	}

	var nameLen int16
	if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
		return "", 0, 0, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return "", 0, 0, err
	}

	var partitions, replicas int32
	if err := binary.Read(buf, binary.BigEndian, &partitions); err != nil {
		return "", 0, 0, err
	}
	if err := binary.Read(buf, binary.BigEndian, &replicas); err != nil {
		return "", 0, 0, err
	}

	return string(nameBytes), partitions, replicas, nil
}

// ListTopicsHandler handles list topics requests
type ListTopicsHandler struct{}

func (h *ListTopicsHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	// Validate request data length
	if len(requestData) < 2 {
		return fmt.Errorf("invalid request data length")
	}

	result, err := cs.broker.Controller.QueryMetadata(protocol.RaftQueryGetTopics, nil)
	if err != nil {
		return fmt.Errorf("failed to get topics: %v", err)
	}

	log.Printf("QueryMetadata returned data length: %d, content: %s", len(result), string(result))

	responseData, err := h.buildListTopicsResponse(result)
	if err != nil {
		return fmt.Errorf("failed to build response: %v", err)
	}

	cs.sendSuccessResponse(conn, responseData)
	log.Printf("Successfully listed topics")
	return nil
}

func (h *ListTopicsHandler) buildListTopicsResponse(topicsData []byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return nil, err
	}

	var topics map[string]*raft.TopicMetadata
	if len(topicsData) > 0 {
		if err := json.Unmarshal(topicsData, &topics); err != nil {
			log.Printf("Failed to unmarshal topics data: %v", err)
			topics = make(map[string]*raft.TopicMetadata)
		}
	} else {
		topics = make(map[string]*raft.TopicMetadata)
	}

	topicCount := int32(len(topics))
	if err := binary.Write(buf, binary.BigEndian, topicCount); err != nil {
		return nil, err
	}

	for topicName, topicMeta := range topics {
		nameBytes := []byte(topicName)
		if err := binary.Write(buf, binary.BigEndian, int16(len(nameBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(nameBytes); err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, topicMeta.Partitions); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, topicMeta.ReplicationFactor); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, topicMeta.CreatedAt.Unix()); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// sendOrderedProduceErrorResponse sends JSON error response for ordered produce requests
func (cs *ClientServer) sendOrderedProduceErrorResponse(conn net.Conn, errorMsg string) error {
	// Send complete PartitionProduceResponse structure that client expects
	errorResponse := struct {
		Partition int32         `json:"partition"`
		Results   []interface{} `json:"results"`
		ErrorCode int16         `json:"error_code"`
		Error     string        `json:"error,omitempty"`
	}{
		Partition: -1,              // -1 indicates error at request level
		Results:   []interface{}{}, // empty results array
		ErrorCode: -1,
		Error:     errorMsg,
	}

	responseData, err := json.Marshal(errorResponse)
	if err != nil {
		log.Printf("Failed to marshal error response: %v", err)
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	responseLen := int32(len(responseData))
	if err := binary.Write(conn, binary.BigEndian, responseLen); err != nil {
		return err
	}
	_, err = conn.Write(responseData)
	return err
}

// DeleteTopicHandler handles delete topic requests
type DeleteTopicHandler struct{}

func (h *DeleteTopicHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	topicName, err := h.parseDeleteTopicRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	if topicName == "" {
		return fmt.Errorf("topic name cannot be empty")
	}

	if cs.broker.Controller == nil {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}

	err = cs.broker.Controller.DeleteTopic(topicName)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %v", err)
	}

	// Send success response
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return fmt.Errorf("failed to write success response: %v", err)
	}

	cs.sendSuccessResponse(conn, buf.Bytes())
	log.Printf("Successfully deleted topic '%s'", topicName)
	return nil
}

func (h *DeleteTopicHandler) parseDeleteTopicRequest(data []byte) (string, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", err
	}

	var nameLen int16
	if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
		return "", err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return "", err
	}

	return string(nameBytes), nil
}

// GetTopicInfoHandler handles get topic info requests
type GetTopicInfoHandler struct{}

func (h *GetTopicInfoHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	topicName, err := h.parseGetTopicInfoRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	topicInfo, err := cs.broker.Controller.GetTopicInfo(topicName)
	if err != nil {
		errorResponse, buildErr := h.buildGetTopicInfoErrorResponse(err.Error())
		if buildErr != nil {
			return fmt.Errorf("failed to build error response: %v", buildErr)
		}
		cs.sendSuccessResponse(conn, errorResponse)
		log.Printf("Topic '%s' not found: %v", topicName, err)
		return nil
	}

	responseData, err := h.buildGetTopicInfoResponse(topicInfo, cs)
	if err != nil {
		return fmt.Errorf("failed to build response: %v", err)
	}

	cs.sendSuccessResponse(conn, responseData)
	log.Printf("Successfully got topic info for '%s'", topicName)
	return nil
}

func (h *GetTopicInfoHandler) parseGetTopicInfoRequest(data []byte) (string, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", err
	}

	var nameLen int16
	if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
		return "", err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return "", err
	}

	return string(nameBytes), nil
}

func (h *GetTopicInfoHandler) buildGetTopicInfoResponse(topicInfo *raft.TopicMetadata, cs *ClientServer) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return nil, err
	}

	nameBytes := []byte(topicInfo.Name)
	if err := binary.Write(buf, binary.BigEndian, int16(len(nameBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(nameBytes); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, topicInfo.Partitions); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, topicInfo.ReplicationFactor); err != nil {
		return nil, err
	}

	createdAtUnix := topicInfo.CreatedAt.Unix()
	if err := binary.Write(buf, binary.BigEndian, createdAtUnix); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, topicInfo.Partitions); err != nil {
		return nil, err
	}

	for i := int32(0); i < topicInfo.Partitions; i++ {
		if err := binary.Write(buf, binary.BigEndian, i); err != nil {
			return nil, err
		}

		var leaderNodeID int32 = 0
		if cs.broker.Controller != nil {
			leaderBrokerID, err := cs.broker.Controller.GetPartitionLeader(topicInfo.Name, i)
			if err == nil && leaderBrokerID != "" {
				// Convert broker ID to node ID for client response
				nodeID := cs.brokerIDToNodeID(leaderBrokerID)
				leaderNodeID = int32(nodeID)
			}
		}

		if err := binary.Write(buf, binary.BigEndian, leaderNodeID); err != nil {
			return nil, err
		}
		partitionSize, messageCount, startOffset, endOffset := cs.getPartitionStatistics(topicInfo.Name, i)
		// return 0 if failed ,it's okay ,we don't want to return err if one partition is failed
		if err := binary.Write(buf, binary.BigEndian, partitionSize); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, messageCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, startOffset); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, endOffset); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (cs *ClientServer) getPartitionStatistics(topicName string, partitionID int32) (int64, int64, int64, int64) {
	raftGroupID := cs.generateRaftGroupID(topicName, partitionID)

	sm, err := cs.broker.raftManager.GetStateMachine(raftGroupID)
	if err != nil {
		return 0, 0, 0, 0
	}

	psm, ok := sm.(*raft.PartitionStateMachine)
	if !ok {
		return 0, 0, 0, 0
	}

	metrics := psm.GetMetrics()

	var partitionSize, messageCount int64
	if bytesStored, ok := metrics["bytes_stored"].(int64); ok {
		partitionSize = bytesStored
	}
	if msgCount, ok := metrics["message_count"].(int64); ok {
		messageCount = msgCount
	}

	var startOffset, endOffset int64
	startOffset = 0
	endOffset = messageCount

	return partitionSize, messageCount, startOffset, endOffset
}

// generateRaftGroupID generates a raft group ID for a topic-partition pair
func (cs *ClientServer) generateRaftGroupID(topicName string, partitionID int32) uint64 {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%s-%d", topicName, partitionID)))
	return h.Sum64()
}

func (h *GetTopicInfoHandler) buildGetTopicInfoErrorResponse(errorMsg string) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, protocol.ErrorInternalError); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(0)); err != nil { // partitions
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int32(0)); err != nil { // replicas
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil { // created time
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil { // size
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil { // message count
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(0)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// JoinGroupHandler handles join group requests
type JoinGroupHandler struct{}

func (h *JoinGroupHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	groupID, memberID, clientID, topics, sessionTimeoutMs, err := h.parseJoinGroupRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	if groupID == "" {
		return fmt.Errorf("group ID cannot be empty")
	}
	if memberID == "" {
		return fmt.Errorf("member ID cannot be empty")
	}

	sessionTimeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	response, err := cs.broker.ConsumerGroupManager.JoinGroup(groupID, memberID, clientID, topics, sessionTimeout)
	if err != nil {
		return fmt.Errorf("failed to join group: %v", err)
	}

	responseData, err := h.buildJoinGroupResponse(response)
	if err != nil {
		return fmt.Errorf("failed to build response: %v", err)
	}

	cs.sendSuccessResponse(conn, responseData)
	log.Printf("Successfully joined group '%s' with member '%s'", groupID, memberID)
	return nil
}

func (h *JoinGroupHandler) parseJoinGroupRequest(data []byte) (string, string, string, []string, int32, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", "", "", nil, 0, err
	}

	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return "", "", "", nil, 0, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(buf, groupIDBytes); err != nil {
		return "", "", "", nil, 0, err
	}

	var memberIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &memberIDLen); err != nil {
		return "", "", "", nil, 0, err
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(buf, memberIDBytes); err != nil {
		return "", "", "", nil, 0, err
	}

	var clientIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &clientIDLen); err != nil {
		return "", "", "", nil, 0, err
	}
	clientIDBytes := make([]byte, clientIDLen)
	if _, err := io.ReadFull(buf, clientIDBytes); err != nil {
		return "", "", "", nil, 0, err
	}

	var topicsCount int32
	if err := binary.Read(buf, binary.BigEndian, &topicsCount); err != nil {
		return "", "", "", nil, 0, err
	}

	topics := make([]string, topicsCount)
	for i := int32(0); i < topicsCount; i++ {
		var topicLen int16
		if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
			return "", "", "", nil, 0, err
		}
		topicBytes := make([]byte, topicLen)
		if _, err := io.ReadFull(buf, topicBytes); err != nil {
			return "", "", "", nil, 0, err
		}
		topics[i] = string(topicBytes)
	}

	var sessionTimeoutMs int32
	if err := binary.Read(buf, binary.BigEndian, &sessionTimeoutMs); err != nil {
		return "", "", "", nil, 0, err
	}

	return string(groupIDBytes), string(memberIDBytes), string(clientIDBytes), topics, sessionTimeoutMs, nil
}

func (h *JoinGroupHandler) buildJoinGroupResponse(response *JoinGroupResponse) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Error code
	if err := binary.Write(buf, binary.BigEndian, response.ErrorCode); err != nil {
		return nil, err
	}

	// Generation
	if err := binary.Write(buf, binary.BigEndian, response.Generation); err != nil {
		return nil, err
	}

	// Group ID
	if err := binary.Write(buf, binary.BigEndian, int16(len(response.GroupID))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(response.GroupID); err != nil {
		return nil, err
	}

	// Member ID
	if err := binary.Write(buf, binary.BigEndian, int16(len(response.MemberID))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(response.MemberID); err != nil {
		return nil, err
	}

	// Leader ID
	if err := binary.Write(buf, binary.BigEndian, int16(len(response.LeaderID))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(response.LeaderID); err != nil {
		return nil, err
	}

	// Members count
	if err := binary.Write(buf, binary.BigEndian, int32(len(response.Members))); err != nil {
		return nil, err
	}
	for _, member := range response.Members {
		if err := binary.Write(buf, binary.BigEndian, int16(len(member.MemberID))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(member.MemberID); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int16(len(member.ClientID))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(member.ClientID); err != nil {
			return nil, err
		}
	}

	// Assignment count (for this member only, assuming single topic)
	if err := binary.Write(buf, binary.BigEndian, int32(1)); err != nil { // Assume single topic for now
		return nil, err
	}

	// Topic name (hardcoded for now, should be from request)
	topicName := "consumer-group-topic" // This should come from the actual topics
	if err := binary.Write(buf, binary.BigEndian, int16(len(topicName))); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(topicName); err != nil {
		return nil, err
	}

	// Partition count for this member
	if err := binary.Write(buf, binary.BigEndian, int32(len(response.Assignment))); err != nil {
		return nil, err
	}
	for _, partition := range response.Assignment {
		if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// LeaveGroupHandler handles leave group requests
type LeaveGroupHandler struct{}

func (h *LeaveGroupHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	groupID, memberID, err := h.parseLeaveGroupRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	if groupID == "" {
		return fmt.Errorf("group ID cannot be empty")
	}
	if memberID == "" {
		return fmt.Errorf("member ID cannot be empty")
	}

	err = cs.broker.ConsumerGroupManager.LeaveGroup(groupID, memberID)
	if err != nil {
		return fmt.Errorf("failed to leave group: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return fmt.Errorf("failed to write success response: %v", err)
	}

	cs.sendSuccessResponse(conn, buf.Bytes())
	log.Printf("Successfully left group '%s' with member '%s'", groupID, memberID)
	return nil
}

func (h *LeaveGroupHandler) parseLeaveGroupRequest(data []byte) (string, string, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", "", err
	}

	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return "", "", err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(buf, groupIDBytes); err != nil {
		return "", "", err
	}

	var memberIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &memberIDLen); err != nil {
		return "", "", err
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(buf, memberIDBytes); err != nil {
		return "", "", err
	}

	return string(groupIDBytes), string(memberIDBytes), nil
}

// GetTopicMetadataHandler handles get topic metadata requests
type GetTopicMetadataHandler struct{}

func (h *GetTopicMetadataHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	topicName, err := h.parseGetTopicMetadataRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	topicMetadata, err := cs.broker.Controller.GetTopicMetadata(topicName)
	if err != nil {
		return fmt.Errorf("topic '%s' not found: %v", topicName, err)
	}

	responseData, err := h.buildGetTopicMetadataResponse(topicMetadata, cs)
	if err != nil {
		return fmt.Errorf("failed to build response: %v", err)
	}

	// Send success response directly here for now
	cs.sendSuccessResponse(conn, responseData)
	log.Printf("Successfully got topic metadata for '%s'", topicName)
	return nil
}

func (h *GetTopicMetadataHandler) parseGetTopicMetadataRequest(data []byte) (string, error) {
	buf := bytes.NewReader(data)

	var nameLen int32
	if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
		return "", err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		return "", err
	}

	return string(nameBytes), nil
}

func (h *GetTopicMetadataHandler) buildGetTopicMetadataResponse(topicMetadata *raft.TopicMetadata, cs *ClientServer) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return nil, err
	}

	var topicMetadataResponse *raft.TopicMetadataResponse
	if cs.broker.Controller != nil && cs.broker.Controller.stateMachine != nil {
		var err error

		if topicMetadataResponse, err = cs.broker.Controller.stateMachine.GetTopicMetadataWithPartitions(topicMetadata.Name); err == nil {
		} else {
			return nil, fmt.Errorf("failed to get topic metadata with partitions: %v", err)
		}
	}

	partitionCount := topicMetadataResponse.Partitions
	if err := binary.Write(buf, binary.BigEndian, partitionCount); err != nil {
		return nil, err
	}

	// Get cluster metadata for broker address resolution
	var clusterMetadata *raft.ClusterMetadata
	if cs.broker.Controller != nil {
		if metadata, err := cs.broker.Controller.GetMetadata(); err == nil {
			clusterMetadata = metadata
		}
	}

	// Write each partition's metadata using the combined response
	for _, partitionInfo := range topicMetadataResponse.PartitionInfos {
		// Write partition ID
		if err := binary.Write(buf, binary.BigEndian, partitionInfo.PartitionID); err != nil {
			return nil, err
		}

		var leaderAddr string
		if partitionInfo.Leader != "" && clusterMetadata != nil {
			if brokerInfo, exists := clusterMetadata.Brokers[partitionInfo.Leader]; exists {
				leaderAddr = fmt.Sprintf("%s:%d", brokerInfo.Address, brokerInfo.Port)
			}
		}

		if err := binary.Write(buf, binary.BigEndian, int32(len(leaderAddr))); err != nil {
			return nil, err
		}
		if _, err := buf.Write([]byte(leaderAddr)); err != nil {
			return nil, err
		}

		// Get replica addresses
		replicaAddrs := []string{"127.0.0.1:9092"} // fallback
		if len(partitionInfo.Replicas) > 0 && clusterMetadata != nil {
			replicaAddrs = make([]string, 0, len(partitionInfo.Replicas))
			for _, replicaID := range partitionInfo.Replicas {
				if brokerInfo, exists := clusterMetadata.Brokers[replicaID]; exists {
					replicaAddr := fmt.Sprintf("%s:%d", brokerInfo.Address, brokerInfo.Port)
					replicaAddrs = append(replicaAddrs, replicaAddr)
				}
			}
		}

		// Write replica count
		replicaCount := int32(len(replicaAddrs))
		if err := binary.Write(buf, binary.BigEndian, replicaCount); err != nil {
			return nil, err
		}

		// Write replica addresses
		for _, replicaAddr := range replicaAddrs {
			if err := binary.Write(buf, binary.BigEndian, int32(len(replicaAddr))); err != nil {
				return nil, err
			}
			if _, err := buf.Write([]byte(replicaAddr)); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

// ProduceHandler handles produce requests
type ProduceHandler struct{}

// parseProduceRequest parses binary produce request from client
func (h *ProduceHandler) parseProduceRequest(data []byte) (*ProduceRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// Read topic length (4 bytes)
	topicLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	if len(data) < offset+int(topicLen)+4 {
		return nil, fmt.Errorf("invalid topic length")
	}

	// Read topic
	topic := string(data[offset : offset+int(topicLen)])
	offset += int(topicLen)

	// Read partition (4 bytes)
	partition := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Read message count (4 bytes)
	if len(data) < offset+4 {
		return nil, fmt.Errorf("invalid message count")
	}
	messageCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	messages := make([]ProduceMessage, messageCount)
	for i := uint32(0); i < messageCount; i++ {
		if len(data) < offset+4 {
			return nil, fmt.Errorf("invalid message %d", i)
		}

		// Read ProducerID length (4 bytes)
		producerIDLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		var producerID string
		if producerIDLen > 0 {
			if len(data) < offset+int(producerIDLen) {
				return nil, fmt.Errorf("invalid producer ID length for message %d", i)
			}
			producerID = string(data[offset : offset+int(producerIDLen)])
			offset += int(producerIDLen)
		}

		// Read SequenceNumber (8 bytes)
		if len(data) < offset+8 {
			return nil, fmt.Errorf("invalid sequence number for message %d", i)
		}
		sequenceNumber := int64(binary.BigEndian.Uint64(data[offset:]))
		offset += 8

		// Read AsyncIO (1 byte)
		if len(data) < offset+1 {
			return nil, fmt.Errorf("invalid async IO flag for message %d", i)
		}
		asyncIO := data[offset] == 1
		offset += 1

		// Read key length (4 bytes)
		if len(data) < offset+4 {
			return nil, fmt.Errorf("invalid key length for message %d", i)
		}
		keyLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		var key []byte
		if keyLen > 0 {
			if len(data) < offset+int(keyLen) {
				return nil, fmt.Errorf("invalid key length for message %d", i)
			}
			key = data[offset : offset+int(keyLen)]
			offset += int(keyLen)
		}

		// Read value length (4 bytes)
		if len(data) < offset+4 {
			return nil, fmt.Errorf("invalid value length for message %d", i)
		}
		valueLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		if len(data) < offset+int(valueLen) {
			return nil, fmt.Errorf("invalid value length for message %d", i)
		}

		// Read value
		value := data[offset : offset+int(valueLen)]
		offset += int(valueLen)

		messages[i] = ProduceMessage{
			ProducerID:     producerID,
			SequenceNumber: sequenceNumber,
			AsyncIO:        asyncIO,
			Key:            key,
			Value:          value,
		}
	}

	return &ProduceRequest{
		Topic:     topic,
		Partition: int32(partition),
		Messages:  messages,
	}, nil
}

func (h *ProduceHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		log.Printf("Failed to read produce request: %v", err)
		return fmt.Errorf("Failed to read request: %v", err)
	}

	// Parse binary produce request from client
	produceReq, err := h.parseProduceRequest(requestData)
	if err != nil {
		log.Printf("Failed to parse produce request: %v", err)
		return fmt.Errorf("Invalid request format: %v", err)
	}

	log.Printf("Handling produce request for topic %s, partition %d", produceReq.Topic, produceReq.Partition)

	partitionKey := fmt.Sprintf("%s-%d", produceReq.Topic, produceReq.Partition)
	leader, err := cs.findPartitionLeader(partitionKey)
	if err != nil {
		log.Printf("Failed to find partition leader: %v", err)
		leaderErr := &errors.TypedError{
			Type:    errors.PartitionLeaderError,
			Message: fmt.Sprintf("Partition leader not found: %v", err),
			Cause:   err,
		}
		return leaderErr
	}
	//must be leader
	if leader != cs.broker.ID {
		log.Printf("Not the leader for partition %s, leader is %s", partitionKey, leader)
		leaderErr := &errors.TypedError{
			Type:    errors.PartitionLeaderError,
			Message: fmt.Sprintf("Not the leader, leader is %s", leader),
		}
		return leaderErr
	}

	response, err := cs.handleProduceToRaft(produceReq)
	if err != nil {
		log.Printf("Failed to produce to Raft: %v", err)
		return fmt.Errorf("Failed to produce: %v", err)
	}

	// Send produce response
	cs.sendProduceResponse(conn, response)
	return nil
}

// FetchHandler handles fetch requests
type FetchHandler struct{}

func (h *FetchHandler) Handle(conn net.Conn, cs *ClientServer) error {
	// Read the fetch request data
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %w", err)
	}

	// Parse the fetch request using binary format
	fetchReq, err := h.parseFetchRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse fetch request: %w", err)
	}

	log.Printf("Handling fetch request for topic %s, partition %d, offset %d",
		fetchReq.Topic, fetchReq.Partition, fetchReq.Offset)

	partitionKey := fmt.Sprintf("%s-%d", fetchReq.Topic, fetchReq.Partition)
	response, err := cs.handleFetchFromRaft(fetchReq, partitionKey)
	if err != nil {
		return fmt.Errorf("failed to handle fetch from raft: %w", err)
	}

	// Send fetch response
	err = cs.sendFetchResponse(conn, response)
	return nil
}

// parseFetchRequest parses binary fetch request from client
func (h *FetchHandler) parseFetchRequest(data []byte) (*FetchRequest, error) {
	buf := bytes.NewReader(data)

	// Read version (2 bytes)
	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %v", err)
	}

	// Read topic length (2 bytes)
	var topicLen int16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}

	// Read topic
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(buf, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic: %v", err)
	}

	// Read partition (4 bytes)
	var partition int32
	if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %v", err)
	}

	// Read offset (8 bytes)
	var offset int64
	if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
		return nil, fmt.Errorf("failed to read offset: %v", err)
	}

	// Read max bytes (4 bytes)
	var maxBytes int32
	if err := binary.Read(buf, binary.BigEndian, &maxBytes); err != nil {
		return nil, fmt.Errorf("failed to read max bytes: %v", err)
	}

	return &FetchRequest{
		Topic:     string(topicBytes),
		Partition: partition,
		Offset:    offset,
		MaxBytes:  maxBytes,
	}, nil
}

// StartPartitionRaftGroupHandler handles requests to start partition Raft groups
type StartPartitionRaftGroupHandler struct{}

func (h *StartPartitionRaftGroupHandler) Handle(conn net.Conn, cs *ClientServer) error {
	// Read request data length
	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		return fmt.Errorf("failed to read data length: %v", err)
	}

	// Read request data
	requestData := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, requestData); err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	// Parse request
	var request raft.StartPartitionRaftGroupRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	log.Printf("Received StartPartitionRaftGroup request: GroupID=%d, Topic=%s, Partition=%d, Join=%t",
		request.RaftGroupID, request.TopicName, request.PartitionID, request.Join)

	// Start the partition Raft group
	response := h.startPartitionRaftGroup(cs, &request)

	// Send response
	return h.sendResponse(conn, response)
}

func (h *StartPartitionRaftGroupHandler) startPartitionRaftGroup(
	cs *ClientServer,
	request *raft.StartPartitionRaftGroupRequest,
) *raft.StartPartitionRaftGroupResponse {
	// Create partition state machine (use broker's data directory)
	stateMachine, err := raft.NewPartitionStateMachine(
		request.TopicName,
		request.PartitionID,
		cs.broker.Config.DataDir,
		cs.broker.GetCompressor(),
	)
	if err != nil {
		return &raft.StartPartitionRaftGroupResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to create partition state machine: %v", err),
		}
	}

	// Start the Raft group
	err = cs.broker.raftManager.StartRaftGroup(
		request.RaftGroupID,
		request.NodeMembers,
		stateMachine,
		request.Join,
	)

	if err != nil {
		log.Printf("Failed to start Raft group %d: %v", request.RaftGroupID, err)
		return &raft.StartPartitionRaftGroupResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	log.Printf("Successfully started Raft group %d for partition %s-%d (join=%t)",
		request.RaftGroupID, request.TopicName, request.PartitionID, request.Join)

	return &raft.StartPartitionRaftGroupResponse{
		Success: true,
		Message: fmt.Sprintf("Raft group %d started successfully", request.RaftGroupID),
	}
}

func (h *StartPartitionRaftGroupHandler) sendResponse(conn net.Conn, response *raft.StartPartitionRaftGroupResponse) error {
	responseData, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to serialize response: %v", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(responseData))); err != nil {
		return fmt.Errorf("failed to write response length: %v", err)
	}

	if _, err := conn.Write(responseData); err != nil {
		return fmt.Errorf("failed to write response data: %v", err)
	}

	return nil
}

// StopPartitionRaftGroupHandler handles requests to stop partition Raft groups
type StopPartitionRaftGroupHandler struct{}

func (h *StopPartitionRaftGroupHandler) Handle(conn net.Conn, cs *ClientServer) error {
	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		return fmt.Errorf("failed to read data length: %v", err)
	}

	requestData := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, requestData); err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	var request raft.StopPartitionRaftGroupRequest
	if err := json.Unmarshal(requestData, &request); err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	log.Printf("Received StopPartitionRaftGroup request: GroupID=%d, Topic=%s, Partition=%d",
		request.RaftGroupID, request.TopicName, request.PartitionID)

	response := h.stopPartitionRaftGroup(cs, &request)

	return h.sendResponse(conn, response)
}

func (h *StopPartitionRaftGroupHandler) stopPartitionRaftGroup(
	cs *ClientServer,
	request *raft.StopPartitionRaftGroupRequest,
) *raft.StopPartitionRaftGroupResponse {
	err := cs.broker.raftManager.StopRaftGroup(request.RaftGroupID)

	if err != nil {
		log.Printf("Failed to stop Raft group %d: %v", request.RaftGroupID, err)
		return &raft.StopPartitionRaftGroupResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	log.Printf("Successfully stopped Raft group %d for partition %s-%d",
		request.RaftGroupID, request.TopicName, request.PartitionID)

	return &raft.StopPartitionRaftGroupResponse{
		Success: true,
		Message: fmt.Sprintf("Raft group %d stopped successfully", request.RaftGroupID),
	}
}

func (h *StopPartitionRaftGroupHandler) sendResponse(conn net.Conn, response *raft.StopPartitionRaftGroupResponse) error {
	responseData, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to serialize response: %v", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(responseData))); err != nil {
		return fmt.Errorf("failed to write response length: %v", err)
	}

	if _, err := conn.Write(responseData); err != nil {
		return fmt.Errorf("failed to write response data: %v", err)
	}

	return nil
}

// HeartbeatHandler handles heartbeat requests
type HeartbeatHandler struct{}

func (h *HeartbeatHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	groupID, memberID, generation, err := h.parseHeartbeatRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	response, err := cs.broker.ConsumerGroupManager.Heartbeat(groupID, memberID, generation)
	if err != nil {
		return fmt.Errorf("failed to process heartbeat: %v", err)
	}

	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, response.ErrorCode); err != nil {
		return fmt.Errorf("failed to write error code: %v", err)
	}

	if err := binary.Write(buf, binary.BigEndian, response.Generation); err != nil {
		return fmt.Errorf("failed to write generation: %v", err)
	}

	rebalanceFlag := int8(0)
	if response.NeedRebalance {
		rebalanceFlag = 1
	}
	if err := binary.Write(buf, binary.BigEndian, rebalanceFlag); err != nil {
		return fmt.Errorf("failed to write rebalance flag: %v", err)
	}

	if err := binary.Write(buf, binary.BigEndian, response.MemberCount); err != nil {
		return fmt.Errorf("failed to write member count: %v", err)
	}

	leaderIDBytes := []byte(response.LeaderID)
	if err := binary.Write(buf, binary.BigEndian, int16(len(leaderIDBytes))); err != nil {
		return fmt.Errorf("failed to write leader ID length: %v", err)
	}
	if _, err := buf.Write(leaderIDBytes); err != nil {
		return fmt.Errorf("failed to write leader ID: %v", err)
	}

	cs.sendSuccessResponse(conn, buf.Bytes())
	return nil
}

func (h *HeartbeatHandler) parseHeartbeatRequest(data []byte) (string, string, int32, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", "", 0, err
	}

	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return "", "", 0, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(buf, groupIDBytes); err != nil {
		return "", "", 0, err
	}

	var memberIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &memberIDLen); err != nil {
		return "", "", 0, err
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(buf, memberIDBytes); err != nil {
		return "", "", 0, err
	}

	var generation int32
	if err := binary.Read(buf, binary.BigEndian, &generation); err != nil {
		return "", "", 0, err
	}

	return string(groupIDBytes), string(memberIDBytes), generation, nil
}

// CommitOffsetHandler handles commit offset requests
type CommitOffsetHandler struct{}

func (h *CommitOffsetHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	groupID, topic, partition, offset, metadata, err := h.parseCommitOffsetRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	err = cs.broker.ConsumerGroupManager.CommitOffset(groupID, topic, partition, offset, metadata)
	if err != nil {
		return fmt.Errorf("failed to commit offset: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return fmt.Errorf("failed to write success response: %v", err)
	}

	cs.sendSuccessResponse(conn, buf.Bytes())
	log.Printf("Successfully committed offset: group=%s, topic=%s, partition=%d, offset=%d", groupID, topic, partition, offset)
	return nil
}

func (h *CommitOffsetHandler) parseCommitOffsetRequest(data []byte) (string, string, int32, int64, string, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", "", 0, 0, "", err
	}

	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return "", "", 0, 0, "", err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(buf, groupIDBytes); err != nil {
		return "", "", 0, 0, "", err
	}

	var topicLen int16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return "", "", 0, 0, "", err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(buf, topicBytes); err != nil {
		return "", "", 0, 0, "", err
	}

	var partition int32
	if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
		return "", "", 0, 0, "", err
	}

	var offset int64
	if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
		return "", "", 0, 0, "", err
	}

	var metadataLen int16
	if err := binary.Read(buf, binary.BigEndian, &metadataLen); err != nil {
		return "", "", 0, 0, "", err
	}
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(buf, metadataBytes); err != nil {
		return "", "", 0, 0, "", err
	}

	return string(groupIDBytes), string(topicBytes), partition, offset, string(metadataBytes), nil
}

// FetchOffsetHandler handles fetch offset requests
type FetchOffsetHandler struct{}

func (h *FetchOffsetHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	groupID, topic, partition, err := h.parseFetchOffsetRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	offset, err := cs.broker.ConsumerGroupManager.FetchCommittedOffset(groupID, topic, partition)
	if err != nil {
		return fmt.Errorf("failed to fetch offset: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(protocol.ErrorNone)); err != nil {
		return fmt.Errorf("failed to write success response: %v", err)
	}
	if err := binary.Write(buf, binary.BigEndian, offset); err != nil {
		return fmt.Errorf("failed to write offset: %v", err)
	}

	cs.sendSuccessResponse(conn, buf.Bytes())
	return nil
}

func (h *FetchOffsetHandler) parseFetchOffsetRequest(data []byte) (string, string, int32, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", "", 0, err
	}

	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return "", "", 0, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(buf, groupIDBytes); err != nil {
		return "", "", 0, err
	}

	var topicLen int16
	if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return "", "", 0, err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(buf, topicBytes); err != nil {
		return "", "", 0, err
	}

	var partition int32
	if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
		return "", "", 0, err
	}

	return string(groupIDBytes), string(topicBytes), partition, nil
}

type FetchAssignmentHandler struct{}

func (h *FetchAssignmentHandler) Handle(conn net.Conn, cs *ClientServer) error {
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	groupID, memberID, generation, err := h.parseFetchAssignmentRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	response, err := cs.broker.ConsumerGroupManager.FetchAssignment(groupID, memberID, generation)
	if err != nil {
		return fmt.Errorf("failed to fetch assignment: %v", err)
	}

	responseData, err := h.buildFetchAssignmentResponse(response)
	if err != nil {
		return fmt.Errorf("failed to build response: %v", err)
	}

	cs.sendSuccessResponse(conn, responseData)
	return nil
}

func (h *FetchAssignmentHandler) parseFetchAssignmentRequest(data []byte) (string, string, int32, error) {
	buf := bytes.NewReader(data)

	var version int16
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return "", "", 0, err
	}

	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return "", "", 0, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(buf, groupIDBytes); err != nil {
		return "", "", 0, err
	}

	var memberIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &memberIDLen); err != nil {
		return "", "", 0, err
	}
	memberIDBytes := make([]byte, memberIDLen)
	if _, err := io.ReadFull(buf, memberIDBytes); err != nil {
		return "", "", 0, err
	}

	var generation int32
	if err := binary.Read(buf, binary.BigEndian, &generation); err != nil {
		return "", "", 0, err
	}

	return string(groupIDBytes), string(memberIDBytes), generation, nil
}

func (h *FetchAssignmentHandler) buildFetchAssignmentResponse(response *FetchAssignmentResponse) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, response.ErrorCode); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, response.Generation); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(len(response.Assignment))); err != nil {
		return nil, err
	}

	for topic, partitions := range response.Assignment {
		if err := binary.Write(buf, binary.BigEndian, int16(len(topic))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(topic); err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, int32(len(partitions))); err != nil {
			return nil, err
		}

		for _, partition := range partitions {
			if err := binary.Write(buf, binary.BigEndian, partition); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}
