package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"

	"github.com/issac1998/go-queue/internal/metadata"
)

// JoinGroupRequest represents a request to join a consumer group
type JoinGroupRequest struct {
	// GroupID is the ID of the consumer group to join
	GroupID string
	// ConsumerID is the unique ID of the consumer
	ConsumerID string
	// ClientID is the client application identifier
	ClientID string
	// Topics is the list of topics the consumer wants to subscribe to
	Topics []string
	// SessionTimeout is the maximum time the coordinator will wait for heartbeats
	SessionTimeout time.Duration
}

// JoinGroupResponse represents the response from a join group operation
type JoinGroupResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
	// Generation is the current generation ID of the consumer group
	Generation int32
	// GroupID is the ID of the consumer group
	GroupID string
	// ConsumerID is the assigned consumer ID
	ConsumerID string
	// Leader is the ID of the consumer that is the group leader
	Leader string
	// Members is the list of all members in the consumer group
	Members []GroupMember
	// Assignment maps topics to partition assignments for this consumer
	Assignment map[string][]int32 // Topic -> Partitions
}

// GroupMember represents a member of a consumer group
type GroupMember struct {
	// ID is the unique identifier of the group member
	ID string
	// ClientID is the client application identifier
	ClientID string
}

// LeaveGroupRequest represents a request to leave a consumer group
type LeaveGroupRequest struct {
	// GroupID is the ID of the consumer group to leave
	GroupID string
	// ConsumerID is the unique ID of the consumer leaving
	ConsumerID string
}

// LeaveGroupResponse represents the response from a leave group operation
type LeaveGroupResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
}

// HeartbeatRequest represents a heartbeat request to maintain group membership
type HeartbeatRequest struct {
	// GroupID is the ID of the consumer group
	GroupID string
	// ConsumerID is the unique ID of the consumer
	ConsumerID string
	// Generation is the current generation ID of the consumer group
	Generation int32
}

// HeartbeatResponse represents the response from a heartbeat operation
type HeartbeatResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
}

// CommitOffsetRequest represents a request to commit message offsets
type CommitOffsetRequest struct {
	// GroupID is the ID of the consumer group
	GroupID string
	// TopicName is the name of the topic
	TopicName string
	// Partition is the partition ID
	Partition int32
	// Offset is the offset to commit
	Offset int64
	// Metadata is optional metadata associated with the commit
	Metadata string
}

// CommitOffsetResponse represents the response from a commit offset operation
type CommitOffsetResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
}

// FetchOffsetRequest represents a request to fetch committed offsets
type FetchOffsetRequest struct {
	// GroupID is the ID of the consumer group
	GroupID string
	// TopicName is the name of the topic
	TopicName string
	// Partition is the partition ID
	Partition int32
}

// FetchOffsetResponse represents the response from a fetch offset operation
type FetchOffsetResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
	// Offset is the committed offset value
	Offset int64
}

// HandleJoinGroupRequest processes a join group request and writes the response
func HandleJoinGroupRequest(conn net.Conn, manager *metadata.Manager) error {
	req, err := parseJoinGroupRequest(conn)
	if err != nil {
		return sendJoinGroupError(conn, 1) // 协议错误
	}

	log.Printf("Handling JoinGroup request: GroupID=%s, ConsumerID=%s, Topics=%v",
		req.GroupID, req.ConsumerID, req.Topics)

	consumer, err := manager.ConsumerGroups.JoinGroup(
		req.GroupID, req.ConsumerID, req.ClientID, req.Topics, req.SessionTimeout)
	if err != nil {
		log.Printf("Failed to join group: %v", err)
		return sendJoinGroupError(conn, 2) // 服务器错误
	}

	group, exists, unlock := manager.ConsumerGroups.GetGroupInfo(req.GroupID)
	if !exists {
		return sendJoinGroupError(conn, 3) // 组不存在
	}
	needRebalance := group.State == metadata.GroupStatePreparingRebalance
	isLeader := group.Leader == req.ConsumerID
	unlock()

	if needRebalance && isLeader {
		topicPartitions := make(map[string][]int32)

		for _, topicName := range req.Topics {
			if topic, exists := manager.Topics[topicName]; exists {
				var partitions []int32
				for partitionID := range topic.Partitions {
					partitions = append(partitions, partitionID)
				}
				topicPartitions[topicName] = partitions
			}
		}

		err = manager.ConsumerGroups.RebalancePartitions(req.GroupID, topicPartitions)
		if err != nil {
			log.Printf("Failed to rebalance partitions: %v", err)
		}
	}

	group, exists, unlock = manager.ConsumerGroups.GetGroupInfo(req.GroupID)
	if !exists {
		return sendJoinGroupError(conn, 3) // 组不存在
	}
	defer unlock()

	response := &JoinGroupResponse{
		ErrorCode:  0,
		Generation: group.Generation,
		GroupID:    group.ID,
		ConsumerID: consumer.ID,
		Leader:     group.Leader,
		Assignment: consumer.Assignment,
	}

	for _, member := range group.Members {
		response.Members = append(response.Members, GroupMember{
			ID:       member.ID,
			ClientID: member.ClientID,
		})
	}

	return sendJoinGroupResponse(conn, response)
}

// HandleLeaveGroupRequest processes a leave group request and writes the response
func HandleLeaveGroupRequest(conn net.Conn, manager *metadata.Manager) error {
	req, err := parseLeaveGroupRequest(conn)
	if err != nil {
		return sendLeaveGroupError(conn, 1)
	}

	log.Printf("Handling LeaveGroup request: GroupID=%s, ConsumerID=%s", req.GroupID, req.ConsumerID)

	err = manager.ConsumerGroups.LeaveGroup(req.GroupID, req.ConsumerID)
	if err != nil {
		log.Printf("Failed to leave group: %v", err)
		return sendLeaveGroupError(conn, 2)
	}

	return sendLeaveGroupResponse(conn, &LeaveGroupResponse{ErrorCode: 0})
}

// HandleHeartbeatRequest processes a heartbeat request and writes the response
func HandleHeartbeatRequest(conn net.Conn, manager *metadata.Manager) error {
	req, err := parseHeartbeatRequest(conn)
	if err != nil {
		return sendHeartbeatError(conn, 1)
	}

	err = manager.ConsumerGroups.Heartbeat(req.GroupID, req.ConsumerID)
	if err != nil {
		log.Printf("Heartbeat failed: %v", err)
		return sendHeartbeatError(conn, 2)
	}

	return sendHeartbeatResponse(conn, &HeartbeatResponse{ErrorCode: 0})
}

// HandleCommitOffsetRequest processes a commit offset request and writes the response
func HandleCommitOffsetRequest(conn net.Conn, manager *metadata.Manager) error {
	req, err := parseCommitOffsetRequest(conn)
	if err != nil {
		return sendCommitOffsetError(conn, 1)
	}

	log.Printf("Handling CommitOffset request: GroupID=%s, Topic=%s, Partition=%d, Offset=%d",
		req.GroupID, req.TopicName, req.Partition, req.Offset)

	err = manager.ConsumerGroups.CommitOffset(req.GroupID, req.TopicName, req.Partition, req.Offset, req.Metadata)
	if err != nil {
		log.Printf("Failed to commit offset: %v", err)
		return sendCommitOffsetError(conn, 2)
	}

	return sendCommitOffsetResponse(conn, &CommitOffsetResponse{ErrorCode: 0})
}

// HandleFetchOffsetRequest processes a fetch offset request and writes the response
func HandleFetchOffsetRequest(conn net.Conn, manager *metadata.Manager) error {
	req, err := parseFetchOffsetRequest(conn)
	if err != nil {
		return sendFetchOffsetError(conn, 1, -1)
	}

	offset, exists := manager.ConsumerGroups.GetCommittedOffset(req.GroupID, req.TopicName, req.Partition)
	if !exists {
		return sendFetchOffsetError(conn, 3, -1) // Offset不存在
	}

	return sendFetchOffsetResponse(conn, &FetchOffsetResponse{
		ErrorCode: 0,
		Offset:    offset,
	})
}

func parseJoinGroupRequest(conn net.Conn) (*JoinGroupRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	var consumerIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &consumerIDLen); err != nil {
		return nil, err
	}
	consumerIDBytes := make([]byte, consumerIDLen)
	if _, err := io.ReadFull(conn, consumerIDBytes); err != nil {
		return nil, err
	}

	var clientIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &clientIDLen); err != nil {
		return nil, err
	}
	clientIDBytes := make([]byte, clientIDLen)
	if _, err := io.ReadFull(conn, clientIDBytes); err != nil {
		return nil, err
	}

	var topicCount int32
	if err := binary.Read(conn, binary.BigEndian, &topicCount); err != nil {
		return nil, err
	}

	topics := make([]string, topicCount)
	for i := int32(0); i < topicCount; i++ {
		var topicLen int16
		if err := binary.Read(conn, binary.BigEndian, &topicLen); err != nil {
			return nil, err
		}
		topicBytes := make([]byte, topicLen)
		if _, err := io.ReadFull(conn, topicBytes); err != nil {
			return nil, err
		}
		topics[i] = string(topicBytes)
	}

	var sessionTimeoutMs int32
	if err := binary.Read(conn, binary.BigEndian, &sessionTimeoutMs); err != nil {
		return nil, err
	}

	return &JoinGroupRequest{
		GroupID:        string(groupIDBytes),
		ConsumerID:     string(consumerIDBytes),
		ClientID:       string(clientIDBytes),
		Topics:         topics,
		SessionTimeout: time.Duration(sessionTimeoutMs) * time.Millisecond,
	}, nil
}

func parseLeaveGroupRequest(conn net.Conn) (*LeaveGroupRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	var consumerIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &consumerIDLen); err != nil {
		return nil, err
	}
	consumerIDBytes := make([]byte, consumerIDLen)
	if _, err := io.ReadFull(conn, consumerIDBytes); err != nil {
		return nil, err
	}

	return &LeaveGroupRequest{
		GroupID:    string(groupIDBytes),
		ConsumerID: string(consumerIDBytes),
	}, nil
}

func parseHeartbeatRequest(conn net.Conn) (*HeartbeatRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	var consumerIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &consumerIDLen); err != nil {
		return nil, err
	}
	consumerIDBytes := make([]byte, consumerIDLen)
	if _, err := io.ReadFull(conn, consumerIDBytes); err != nil {
		return nil, err
	}

	var generation int32
	if err := binary.Read(conn, binary.BigEndian, &generation); err != nil {
		return nil, err
	}

	return &HeartbeatRequest{
		GroupID:    string(groupIDBytes),
		ConsumerID: string(consumerIDBytes),
		Generation: generation,
	}, nil
}

func parseCommitOffsetRequest(conn net.Conn) (*CommitOffsetRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	var topicLen int16
	if err := binary.Read(conn, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBytes); err != nil {
		return nil, err
	}

	var partition int32
	if err := binary.Read(conn, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	var offset int64
	if err := binary.Read(conn, binary.BigEndian, &offset); err != nil {
		return nil, err
	}

	var metadataLen int16
	if err := binary.Read(conn, binary.BigEndian, &metadataLen); err != nil {
		return nil, err
	}
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(conn, metadataBytes); err != nil {
		return nil, err
	}

	return &CommitOffsetRequest{
		GroupID:   string(groupIDBytes),
		TopicName: string(topicBytes),
		Partition: partition,
		Offset:    offset,
		Metadata:  string(metadataBytes),
	}, nil
}

func parseFetchOffsetRequest(conn net.Conn) (*FetchOffsetRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	var topicLen int16
	if err := binary.Read(conn, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBytes); err != nil {
		return nil, err
	}

	var partition int32
	if err := binary.Read(conn, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	return &FetchOffsetRequest{
		GroupID:   string(groupIDBytes),
		TopicName: string(topicBytes),
		Partition: partition,
	}, nil
}

func sendJoinGroupResponse(conn net.Conn, response *JoinGroupResponse) error {
	buf := new(bytes.Buffer)

	// ErrorCode
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	// Generation
	binary.Write(buf, binary.BigEndian, response.Generation)

	// GroupID
	binary.Write(buf, binary.BigEndian, int16(len(response.GroupID)))
	buf.WriteString(response.GroupID)

	// ConsumerID
	binary.Write(buf, binary.BigEndian, int16(len(response.ConsumerID)))
	buf.WriteString(response.ConsumerID)

	// Leader
	binary.Write(buf, binary.BigEndian, int16(len(response.Leader)))
	buf.WriteString(response.Leader)

	// Members count
	binary.Write(buf, binary.BigEndian, int32(len(response.Members)))
	for _, member := range response.Members {
		binary.Write(buf, binary.BigEndian, int16(len(member.ID)))
		buf.WriteString(member.ID)
		binary.Write(buf, binary.BigEndian, int16(len(member.ClientID)))
		buf.WriteString(member.ClientID)
	}

	// Assignment
	binary.Write(buf, binary.BigEndian, int32(len(response.Assignment)))
	for topic, partitions := range response.Assignment {
		binary.Write(buf, binary.BigEndian, int16(len(topic)))
		buf.WriteString(topic)
		binary.Write(buf, binary.BigEndian, int32(len(partitions)))
		for _, partition := range partitions {
			binary.Write(buf, binary.BigEndian, partition)
		}
	}

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendJoinGroupError(conn net.Conn, errorCode int16) error {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, errorCode)

	binary.Write(buf, binary.BigEndian, int32(0))
	binary.Write(buf, binary.BigEndian, int16(0))
	binary.Write(buf, binary.BigEndian, int16(0))
	binary.Write(buf, binary.BigEndian, int16(0))
	binary.Write(buf, binary.BigEndian, int32(0))

	// Assignment count
	binary.Write(buf, binary.BigEndian, int32(0)) // 没有分配

	responseData := buf.Bytes()
	if err := binary.Write(conn, binary.BigEndian, int32(len(responseData))); err != nil {
		return err
	}
	_, err := conn.Write(responseData)
	return err
}

func sendLeaveGroupResponse(conn net.Conn, response *LeaveGroupResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendLeaveGroupError(conn net.Conn, errorCode int16) error {
	return sendLeaveGroupResponse(conn, &LeaveGroupResponse{ErrorCode: errorCode})
}

func sendHeartbeatResponse(conn net.Conn, response *HeartbeatResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendHeartbeatError(conn net.Conn, errorCode int16) error {
	return sendHeartbeatResponse(conn, &HeartbeatResponse{ErrorCode: errorCode})
}

func sendCommitOffsetResponse(conn net.Conn, response *CommitOffsetResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendCommitOffsetError(conn net.Conn, errorCode int16) error {
	return sendCommitOffsetResponse(conn, &CommitOffsetResponse{ErrorCode: errorCode})
}

func sendFetchOffsetResponse(conn net.Conn, response *FetchOffsetResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)
	binary.Write(buf, binary.BigEndian, response.Offset)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendFetchOffsetError(conn net.Conn, errorCode int16, offset int64) error {
	return sendFetchOffsetResponse(conn, &FetchOffsetResponse{
		ErrorCode: errorCode,
		Offset:    offset,
	})
}
