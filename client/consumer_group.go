package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
)

// GroupConsumer
type GroupConsumer struct {
	client         *Client
	GroupID        string
	ConsumerID     string
	Topics         []string
	SessionTimeout time.Duration

	generation int32
	leader     string
	assignment map[string][]int32
	members    []GroupMember

	heartbeatTicker *time.Ticker
	stopHeartbeat   chan struct{}

	mu sync.RWMutex
}

// GroupMember ...
type GroupMember struct {
	ID       string
	ClientID string
}

// GroupConsumerConfig ...
type GroupConsumerConfig struct {
	GroupID        string
	ConsumerID     string
	Topics         []string
	SessionTimeout time.Duration
}

// NewGroupConsumer create new group consumer
func NewGroupConsumer(client *Client, config GroupConsumerConfig) *GroupConsumer {
	if config.SessionTimeout == 0 {
		config.SessionTimeout = 30 * time.Second
	}

	return &GroupConsumer{
		client:         client,
		GroupID:        config.GroupID,
		ConsumerID:     config.ConsumerID,
		Topics:         config.Topics,
		SessionTimeout: config.SessionTimeout,
		assignment:     make(map[string][]int32),
		stopHeartbeat:  make(chan struct{}),
	}
}

// JoinGroup 加入消费者组
func (gc *GroupConsumer) JoinGroup() error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	log.Printf("Joining consumer group: %s with consumer ID: %s", gc.GroupID, gc.ConsumerID)

	requestData, err := gc.buildJoinGroupRequest()
	if err != nil {
		return fmt.Errorf("failed to build join group request: %v", err)
	}

	responseData, err := gc.client.sendRequest(protocol.JoinGroupRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send join group request: %v", err)
	}

	err = gc.parseJoinGroupResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse join group response: %v", err)
	}

	gc.startHeartbeat()

	log.Printf("Successfully joined group %s, generation: %d, leader: %s",
		gc.GroupID, gc.generation, gc.leader)

	return nil
}

// LeaveGroup 离开消费者组
func (gc *GroupConsumer) LeaveGroup() error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	log.Printf("Leaving consumer group: %s", gc.GroupID)

	// 停止心跳
	gc.stopHeartbeatInternal()

	// 构建请求
	requestData, err := gc.buildLeaveGroupRequest()
	if err != nil {
		return fmt.Errorf("failed to build leave group request: %v", err)
	}

	// 发送请求
	responseData, err := gc.client.sendRequest(protocol.LeaveGroupRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send leave group request: %v", err)
	}

	// 解析响应
	err = gc.parseLeaveGroupResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse leave group response: %v", err)
	}

	log.Printf("Successfully left group %s", gc.GroupID)
	return nil
}

// CommitOffset 提交offset
func (gc *GroupConsumer) CommitOffset(topic string, partition int32, offset int64, metadata string) error {
	log.Printf("Committing offset: group=%s, topic=%s, partition=%d, offset=%d",
		gc.GroupID, topic, partition, offset)

	// 构建请求
	requestData, err := gc.buildCommitOffsetRequest(topic, partition, offset, metadata)
	if err != nil {
		return fmt.Errorf("failed to build commit offset request: %v", err)
	}

	// 发送请求
	responseData, err := gc.client.sendRequest(protocol.CommitOffsetRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send commit offset request: %v", err)
	}

	// 解析响应
	err = gc.parseCommitOffsetResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse commit offset response: %v", err)
	}

	return nil
}

// FetchCommittedOffset 获取已提交的offset
func (gc *GroupConsumer) FetchCommittedOffset(topic string, partition int32) (int64, error) {
	// 构建请求
	requestData, err := gc.buildFetchOffsetRequest(topic, partition)
	if err != nil {
		return -1, fmt.Errorf("failed to build fetch offset request: %v", err)
	}

	// 发送请求
	responseData, err := gc.client.sendRequest(protocol.FetchOffsetRequestType, requestData)
	if err != nil {
		return -1, fmt.Errorf("failed to send fetch offset request: %v", err)
	}

	// 解析响应
	offset, err := gc.parseFetchOffsetResponse(responseData)
	if err != nil {
		return -1, fmt.Errorf("failed to parse fetch offset response: %v", err)
	}

	return offset, nil
}

// GetAssignment 获取分区分配
func (gc *GroupConsumer) GetAssignment() map[string][]int32 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	// 返回副本以避免外部修改
	assignment := make(map[string][]int32)
	for topic, partitions := range gc.assignment {
		partitionsCopy := make([]int32, len(partitions))
		copy(partitionsCopy, partitions)
		assignment[topic] = partitionsCopy
	}
	return assignment
}

// 内部方法

func (gc *GroupConsumer) startHeartbeat() {
	//tolerate 3 times.
	gc.heartbeatTicker = time.NewTicker(gc.SessionTimeout / 3)

	go func() {
		for {
			select {
			case <-gc.heartbeatTicker.C:
				if err := gc.sendHeartbeat(); err != nil {
					log.Printf("Heartbeat failed: %v", err)
				}
			case <-gc.stopHeartbeat:
				return
			}
		}
	}()
}

func (gc *GroupConsumer) stopHeartbeatInternal() {
	if gc.heartbeatTicker != nil {
		gc.heartbeatTicker.Stop()
		close(gc.stopHeartbeat)
		gc.stopHeartbeat = make(chan struct{}) // 重新创建以便后续使用
	}
}

func (gc *GroupConsumer) sendHeartbeat() error {
	requestData, err := gc.buildHeartbeatRequest()
	if err != nil {
		return fmt.Errorf("failed to build heartbeat request: %v", err)
	}

	responseData, err := gc.client.sendRequest(protocol.HeartbeatRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat request: %v", err)
	}

	// 解析响应
	return gc.parseHeartbeatResponse(responseData)
}

func (gc *GroupConsumer) buildJoinGroupRequest() ([]byte, error) {
	buf := new(bytes.Buffer)

	// 协议版本
	binary.Write(buf, binary.BigEndian, int16(1))

	// GroupID
	binary.Write(buf, binary.BigEndian, int16(len(gc.GroupID)))
	buf.WriteString(gc.GroupID)

	// ConsumerID
	binary.Write(buf, binary.BigEndian, int16(len(gc.ConsumerID)))
	buf.WriteString(gc.ConsumerID)

	// ClientID
	clientID := "go-queue-client"
	binary.Write(buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)

	// Topics
	binary.Write(buf, binary.BigEndian, int32(len(gc.Topics)))
	for _, topic := range gc.Topics {
		binary.Write(buf, binary.BigEndian, int16(len(topic)))
		buf.WriteString(topic)
	}

	sessionTimeoutMs := int32(gc.SessionTimeout / time.Millisecond)
	binary.Write(buf, binary.BigEndian, sessionTimeoutMs)

	return buf.Bytes(), nil
}

func (gc *GroupConsumer) buildLeaveGroupRequest() ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, int16(1))

	binary.Write(buf, binary.BigEndian, int16(len(gc.GroupID)))
	buf.WriteString(gc.GroupID)

	binary.Write(buf, binary.BigEndian, int16(len(gc.ConsumerID)))
	buf.WriteString(gc.ConsumerID)

	return buf.Bytes(), nil
}

func (gc *GroupConsumer) buildHeartbeatRequest() ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, int16(1))

	binary.Write(buf, binary.BigEndian, int16(len(gc.GroupID)))
	buf.WriteString(gc.GroupID)

	binary.Write(buf, binary.BigEndian, int16(len(gc.ConsumerID)))
	buf.WriteString(gc.ConsumerID)

	binary.Write(buf, binary.BigEndian, gc.generation)

	return buf.Bytes(), nil
}

func (gc *GroupConsumer) buildCommitOffsetRequest(topic string, partition int32, offset int64, metadata string) ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, int16(1))

	binary.Write(buf, binary.BigEndian, int16(len(gc.GroupID)))
	buf.WriteString(gc.GroupID)

	binary.Write(buf, binary.BigEndian, int16(len(topic)))
	buf.WriteString(topic)

	binary.Write(buf, binary.BigEndian, partition)

	binary.Write(buf, binary.BigEndian, offset)

	binary.Write(buf, binary.BigEndian, int16(len(metadata)))
	buf.WriteString(metadata)

	return buf.Bytes(), nil
}

func (gc *GroupConsumer) buildFetchOffsetRequest(topic string, partition int32) ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, int16(1))

	binary.Write(buf, binary.BigEndian, int16(len(gc.GroupID)))
	buf.WriteString(gc.GroupID)

	binary.Write(buf, binary.BigEndian, int16(len(topic)))
	buf.WriteString(topic)

	binary.Write(buf, binary.BigEndian, partition)

	return buf.Bytes(), nil
}


func (gc *GroupConsumer) parseJoinGroupResponse(data []byte) error {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return err
	}
	if errorCode != 0 {
		return fmt.Errorf("join group failed with error code: %d", errorCode)
	}

	if err := binary.Read(buf, binary.BigEndian, &gc.generation); err != nil {
		return err
	}

	var groupIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &groupIDLen); err != nil {
		return err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(buf, groupIDBytes); err != nil {
		return err
	}

	var consumerIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &consumerIDLen); err != nil {
		return err
	}
	consumerIDBytes := make([]byte, consumerIDLen)
	if _, err := io.ReadFull(buf, consumerIDBytes); err != nil {
		return err
	}

	var leaderLen int16
	if err := binary.Read(buf, binary.BigEndian, &leaderLen); err != nil {
		return err
	}
	leaderBytes := make([]byte, leaderLen)
	if _, err := io.ReadFull(buf, leaderBytes); err != nil {
		return err
	}
	gc.leader = string(leaderBytes)

	var memberCount int32
	if err := binary.Read(buf, binary.BigEndian, &memberCount); err != nil {
		return err
	}

	gc.members = make([]GroupMember, memberCount)
	for i := int32(0); i < memberCount; i++ {
		var memberIDLen int16
		if err := binary.Read(buf, binary.BigEndian, &memberIDLen); err != nil {
			return err
		}
		memberIDBytes := make([]byte, memberIDLen)
		if _, err := io.ReadFull(buf, memberIDBytes); err != nil {
			return err
		}

		var clientIDLen int16
		if err := binary.Read(buf, binary.BigEndian, &clientIDLen); err != nil {
			return err
		}
		clientIDBytes := make([]byte, clientIDLen)
		if _, err := io.ReadFull(buf, clientIDBytes); err != nil {
			return err
		}

		gc.members[i] = GroupMember{
			ID:       string(memberIDBytes),
			ClientID: string(clientIDBytes),
		}
	}

	var assignmentCount int32
	if err := binary.Read(buf, binary.BigEndian, &assignmentCount); err != nil {
		return err
	}

	gc.assignment = make(map[string][]int32)
	for i := int32(0); i < assignmentCount; i++ {
		var topicLen int16
		if err := binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
			return err
		}
		topicBytes := make([]byte, topicLen)
		if _, err := io.ReadFull(buf, topicBytes); err != nil {
			return err
		}
		topic := string(topicBytes)

		var partitionCount int32
		if err := binary.Read(buf, binary.BigEndian, &partitionCount); err != nil {
			return err
		}

		partitions := make([]int32, partitionCount)
		for j := int32(0); j < partitionCount; j++ {
			if err := binary.Read(buf, binary.BigEndian, &partitions[j]); err != nil {
				return err
			}
		}
		gc.assignment[topic] = partitions
	}

	return nil
}

func (gc *GroupConsumer) parseLeaveGroupResponse(data []byte) error {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return err
	}
	if errorCode != 0 {
		return fmt.Errorf("leave group failed with error code: %d", errorCode)
	}

	return nil
}

func (gc *GroupConsumer) parseHeartbeatResponse(data []byte) error {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return err
	}
	if errorCode != 0 {
		return fmt.Errorf("heartbeat failed with error code: %d", errorCode)
	}

	return nil
}

func (gc *GroupConsumer) parseCommitOffsetResponse(data []byte) error {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return err
	}
	if errorCode != 0 {
		return fmt.Errorf("commit offset failed with error code: %d", errorCode)
	}

	return nil
}

func (gc *GroupConsumer) parseFetchOffsetResponse(data []byte) (int64, error) {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return -1, err
	}
	if errorCode != 0 {
		return -1, fmt.Errorf("fetch offset failed with error code: %d", errorCode)
	}

	var offset int64
	if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
		return -1, err
	}

	return offset, nil
}
