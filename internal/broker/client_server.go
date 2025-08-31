package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"time"

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
)

// RequestConfig defines configuration for each request type
type RequestConfig struct {
	Type    RequestType
	Handler RequestHandler
}

// requestConfigs maps request types to their configurations
var requestConfigs = map[int32]RequestConfig{
	protocol.ControllerDiscoverRequestType: {Type: ControllerRequest, Handler: &ControllerDiscoveryHandler{}},
	protocol.ControllerVerifyRequestType:   {Type: ControllerRequest, Handler: &ControllerVerifyHandler{}},
	protocol.CreateTopicRequestType:        {Type: MetadataWriteRequest, Handler: &CreateTopicHandler{}},
	protocol.DeleteTopicRequestType:        {Type: MetadataWriteRequest, Handler: &DeleteTopicHandler{}},
	protocol.ListTopicsRequestType:         {Type: MetadataReadRequest, Handler: &ListTopicsHandler{}},
	protocol.GetTopicInfoRequestType:       {Type: MetadataReadRequest, Handler: &GetTopicInfoHandler{}},
	protocol.GetTopicMetadataRequestType:   {Type: MetadataReadRequest, Handler: &GetTopicMetadataHandler{}},
	protocol.JoinGroupRequestType:          {Type: MetadataWriteRequest, Handler: &JoinGroupHandler{}},
	protocol.LeaveGroupRequestType:         {Type: MetadataWriteRequest, Handler: &LeaveGroupHandler{}},
	protocol.ProduceRequestType:            {Type: DataRequest, Handler: &ProduceHandler{}},
	protocol.FetchRequestType:              {Type: DataRequest, Handler: &FetchHandler{}},
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
	conn.SetDeadline(time.Now().Add(30 * time.Second))

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
		// TODO: handle data request
		return config.Handler.Handle(conn, cs)

	default:
		return fmt.Errorf("unknown request type category")
	}
}

func (cs *ClientServer) isLeader() bool {
	leaderID, exists, _ := cs.broker.raftManager.GetLeaderID(cs.broker.Config.RaftConfig.ControllerGroupID)
	if exists && leaderID == cs.broker.Controller.brokerIDToNodeID(cs.broker.ID) {
		return true
	}
	return false
}

// handleMetadataWriteRequest handles requests that can only be processed by Controller Leader
func (cs *ClientServer) handleMetadataWriteRequest(conn net.Conn, config RequestConfig) error {
	leaderID, exists := cs.broker.Controller.GetControlledLeaderID()
	if !exists {
		return fmt.Errorf("not controller leader, no leader currently known")
	}
	if leaderID != cs.broker.Controller.brokerIDToNodeID(cs.broker.ID) {
		return fmt.Errorf("not controller leader, please redirect to: %s", cs.getControllerLeaderAddr())
	}

	return config.Handler.Handle(conn, cs)
}

// handleMetadataReadRequest handles metadata read requests with follower read support
func (cs *ClientServer) handleMetadataReadRequest(conn net.Conn, config RequestConfig) error {
	leaderID, exists := cs.broker.Controller.GetControlledLeaderID()
	if exists && leaderID == cs.broker.Controller.brokerIDToNodeID(cs.broker.ID) {
		return config.Handler.Handle(conn, cs)
	}

	isFollowerReadEnabled := cs.broker.Config.EnableFollowerRead

	// Follower can handle if follower read is enabled
	if isFollowerReadEnabled {
		if err := cs.ensureReadIndexConsistency(); err != nil {
			log.Printf("ReadIndex failed: %v", err)
			return fmt.Errorf("read consistency check failed: %v", err)
		}
		return config.Handler.Handle(conn, cs)
	}

	return fmt.Errorf("not controller leader, can't do follower read either")
}

// ensureReadIndexConsistency uses Dragonboat's ReadIndex to ensure read consistency
func (cs *ClientServer) ensureReadIndexConsistency() error {
	if cs.broker.Controller == nil {
		return fmt.Errorf("controller not available")
	}
	// wait readindex catch up for 5 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	groupID := cs.broker.Config.RaftConfig.ControllerGroupID

	_, err := cs.broker.raftManager.EnsureReadIndexConsistency(ctx, groupID)
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
	errorResponse := fmt.Sprintf("ERROR: %v", err)
	responseData := []byte(errorResponse)
	responseLen := int32(len(responseData))

	binary.Write(conn, binary.BigEndian, responseLen)
	conn.Write(responseData)
}

func (cs *ClientServer) sendSuccessResponse(conn net.Conn, data []byte) {
	responseLen := int32(len(data))
	binary.Write(conn, binary.BigEndian, responseLen)
	conn.Write(data)
}

func (cs *ClientServer) readRequestData(conn net.Conn) ([]byte, error) {
	// For simple requests, we may not have explicit length
	// Try to read what's available with a reasonable timeout
	buffer := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read request data: %v", err)
	}
	return buffer[:n], nil
}

// ControllerDiscoveryHandler handles controller discovery requests
type ControllerDiscoveryHandler struct{}

func (h *ControllerDiscoveryHandler) Handle(conn net.Conn, cs *ClientServer) error {
	var controllerAddr string

	if cs.broker.Controller != nil && cs.broker.Controller.isLeader() {
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
	isLeader := cs.broker.Controller != nil

	var response []byte
	if isLeader {
		response = []byte("LEADER")
	} else {
		response = []byte("FOLLOWER")
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
	// Read request data
	// TODO: A better way to read request Data
	requestData, err := cs.readRequestData(conn)
	if err != nil {
		return fmt.Errorf("failed to read request data: %v", err)
	}

	topicName, partitions, replicationFactor, err := h.parseCreateTopicRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	if topicName == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if partitions <= 0 {
		return fmt.Errorf("partitions must be greater than 0")
	}
	if replicationFactor <= 0 {
		return fmt.Errorf("replication factor must be greater than 0")
	}

	err = cs.broker.Controller.CreateTopic(topicName, partitions, replicationFactor)
	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
		return fmt.Errorf("failed to write success response: %v", err)
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
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %v", err)
	}

	result, err := cs.broker.Controller.QueryMetadata(protocol.RaftQueryGetTopics, nil)
	if err != nil {
		return fmt.Errorf("failed to get topics: %v", err)
	}

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

	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
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

	// Write topic count
	topicCount := int32(len(topics))
	if err := binary.Write(buf, binary.BigEndian, topicCount); err != nil {
		return nil, err
	}

	// Write each topic
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

		if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil { // Size
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil { // Message count
			return nil, err
		}
	}

	return buf.Bytes(), nil
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
		return fmt.Errorf("controller not available")
	}

	err = cs.broker.Controller.DeleteTopic(topicName)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %v", err)
	}

	// Send success response
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
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
		return fmt.Errorf("failed to get topic info: %v", err)
	}

	responseData, err := h.buildGetTopicInfoResponse(topicInfo)
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

func (h *GetTopicInfoHandler) buildGetTopicInfoResponse(topicInfo *raft.TopicMetadata) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
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
	if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil { // Message count (placeholder)
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil { // Size (placeholder)
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

	groupID, memberID, err := h.parseJoinGroupRequest(requestData)
	if err != nil {
		return fmt.Errorf("failed to parse request: %v", err)
	}

	if groupID == "" {
		return fmt.Errorf("group ID cannot be empty")
	}
	if memberID == "" {
		return fmt.Errorf("member ID cannot be empty")
	}

	err = cs.broker.Controller.JoinGroup(groupID, memberID)
	if err != nil {
		return fmt.Errorf("failed to join group: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
		return fmt.Errorf("failed to write success response: %v", err)
	}

	cs.sendSuccessResponse(conn, buf.Bytes())
	log.Printf("Successfully joined group '%s' with member '%s'", groupID, memberID)
	return nil
}

func (h *JoinGroupHandler) parseJoinGroupRequest(data []byte) (string, string, error) {
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

	err = cs.broker.Controller.LeaveGroup(groupID, memberID)
	if err != nil {
		return fmt.Errorf("failed to leave group: %v", err)
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
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
		return fmt.Errorf("failed to get topic metadata: %v", err)
	}

	responseData, err := h.buildGetTopicMetadataResponse(topicMetadata)
	if err != nil {
		return fmt.Errorf("failed to build response: %v", err)
	}

	cs.sendSuccessResponse(conn, responseData)
	log.Printf("Successfully got topic metadata for '%s'", topicName)
	return nil
}

func (h *GetTopicMetadataHandler) parseGetTopicMetadataRequest(data []byte) (string, error) {
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

func (h *GetTopicMetadataHandler) buildGetTopicMetadataResponse(topicMetadata *raft.TopicMetadata) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, int16(0)); err != nil {
		return nil, err
	}

	metadataJSON, err := json.Marshal(topicMetadata)
	if err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(len(metadataJSON))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(metadataJSON); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ProduceHandler handles produce requests
type ProduceHandler struct{}

func (h *ProduceHandler) Handle(conn net.Conn, cs *ClientServer) error {
	// TODO: Implement produce - this is a data operation, not metadata
	return fmt.Errorf("produce not implemented yet")
}

// FetchHandler handles fetch requests
type FetchHandler struct{}

func (h *FetchHandler) Handle(conn net.Conn, cs *ClientServer) error {
	// TODO: Implement fetch - this is a data operation, not metadata
	return fmt.Errorf("fetch not implemented yet")
}
