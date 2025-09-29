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

	subscribedTopics map[string]bool //

	rebalancing      bool
	rebalanceTrigger chan struct{}

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

	gc := &GroupConsumer{
		client:           client,
		GroupID:          config.GroupID,
		ConsumerID:       config.ConsumerID,
		Topics:           config.Topics,
		SessionTimeout:   config.SessionTimeout,
		assignment:       make(map[string][]int32),
		members:          make([]GroupMember, 0),
		stopHeartbeat:    make(chan struct{}),
		subscribedTopics: make(map[string]bool),
		rebalanceTrigger: make(chan struct{}, 1),
	}

	for _, topic := range config.Topics {
		gc.subscribedTopics[topic] = true
	}

	return gc
}

func (gc *GroupConsumer) JoinGroup() error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	log.Printf("Joining consumer group: %s with consumer ID: %s", gc.GroupID, gc.ConsumerID)

	gc.rebalancing = true
	defer func() {
		gc.rebalancing = false
	}()

	requestData, err := gc.buildJoinGroupRequest()
	if err != nil {
		return fmt.Errorf("failed to build join group request: %v", err)
	}

	responseData, err := gc.client.sendMetaRequest(protocol.JoinGroupRequestType, requestData)
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

	log.Printf("JoinGroup rebalancing completed for consumer %s", gc.ConsumerID)

	return nil
}

func (gc *GroupConsumer) LeaveGroup() error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	log.Printf("Leaving consumer group: %s", gc.GroupID)

	// 🔥 Clean up local state
	gc.assignment = make(map[string][]int32)
	gc.members = make([]GroupMember, 0)
	gc.generation = 0
	gc.leader = ""

	// Clean up subscription state (but keep subscription info for potential rejoin)
	// gc.subscribedTopics is preserved as it may be needed for rejoining

	gc.stopHeartbeatInternal()

	requestData, err := gc.buildLeaveGroupRequest()
	if err != nil {
		return fmt.Errorf("failed to build leave group request: %v", err)
	}

	responseData, err := gc.client.sendMetaRequest(protocol.LeaveGroupRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send leave group request: %v", err)
	}

	err = gc.parseLeaveGroupResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse leave group response: %v", err)
	}

	log.Printf("Successfully left group %s", gc.GroupID)

	log.Printf("Other consumers in group %s will rebalance automatically", gc.GroupID)

	return nil
}

func (gc *GroupConsumer) CommitOffset(topic string, partition int32, offset int64, metadata string) error {
	log.Printf("Committing offset: group=%s, topic=%s, partition=%d, offset=%d",
		gc.GroupID, topic, partition, offset)

	requestData, err := gc.buildCommitOffsetRequest(topic, partition, offset, metadata)
	if err != nil {
		return fmt.Errorf("failed to build commit offset request: %v", err)
	}

	responseData, err := gc.client.sendMetaRequest(protocol.CommitOffsetRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send commit offset request: %v", err)
	}

	err = gc.parseCommitOffsetResponse(responseData)
	if err != nil {
		return fmt.Errorf("failed to parse commit offset response: %v", err)
	}

	return nil
}

func (gc *GroupConsumer) FetchCommittedOffset(topic string, partition int32) (int64, error) {
	requestData, err := gc.buildFetchOffsetRequest(topic, partition)
	if err != nil {
		return -1, fmt.Errorf("failed to build fetch offset request: %v", err)
	}

	responseData, err := gc.client.sendMetaRequest(protocol.FetchOffsetRequestType, requestData)
	if err != nil {
		return -1, fmt.Errorf("failed to send fetch offset request: %v", err)
	}

	offset, err := gc.parseFetchOffsetResponse(responseData)
	if err != nil {
		return -1, fmt.Errorf("failed to parse fetch offset response: %v", err)
	}

	return offset, nil
}

func (gc *GroupConsumer) GetAssignment() map[string][]int32 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	assignment := make(map[string][]int32)
	for topic, partitions := range gc.assignment {
		partitionsCopy := make([]int32, len(partitions))
		copy(partitionsCopy, partitions)
		assignment[topic] = partitionsCopy
	}
	return assignment
}

// Subscribe dynamically subscribes to new topics
func (gc *GroupConsumer) Subscribe(topics []string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	changed := false
	newTopics := make([]string, 0)

	for _, topic := range topics {
		if !gc.subscribedTopics[topic] {
			gc.subscribedTopics[topic] = true
			gc.Topics = append(gc.Topics, topic)
			newTopics = append(newTopics, topic)
			changed = true
		}
	}

	if changed {
		return gc.triggerRebalance("subscription_changed")
	}

	return nil
}

// Unsubscribe dynamically unsubscribes from topics
func (gc *GroupConsumer) Unsubscribe(topics []string) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	changed := false
	removedTopics := make([]string, 0)

	for _, topicToRemove := range topics {
		if gc.subscribedTopics[topicToRemove] {
			delete(gc.subscribedTopics, topicToRemove)
			removedTopics = append(removedTopics, topicToRemove)

			for i, topic := range gc.Topics {
				if topic == topicToRemove {
					gc.Topics = append(gc.Topics[:i], gc.Topics[i+1:]...)
					break
				}
			}

			delete(gc.assignment, topicToRemove)
			changed = true
		}
	}

	if changed {
		log.Printf("Consumer %s unsubscribed from topics: %v", gc.ConsumerID, removedTopics)
		return gc.triggerRebalance("unsubscription_changed")
	}

	return nil
}

// GetSubscription returns currently subscribed topics
func (gc *GroupConsumer) GetSubscription() []string {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	var subscribed []string
	for topic, isSubscribed := range gc.subscribedTopics {
		if isSubscribed {
			subscribed = append(subscribed, topic)
		}
	}
	return subscribed
}

// IsSubscribed checks if a topic is subscribed
func (gc *GroupConsumer) IsSubscribed(topic string) bool {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.subscribedTopics[topic]
}

// triggerRebalance triggers an automatic rebalance for the consumer group
func (gc *GroupConsumer) triggerRebalance(reason string) error {
	log.Printf("Consumer %s triggering rebalance due to: %s", gc.ConsumerID, reason)

	gc.rebalancing = true

	select {
	case gc.rebalanceTrigger <- struct{}{}:
	default:
		log.Printf("Rebalance already pending for consumer %s", gc.ConsumerID)
	}

	go gc.performRebalance()

	return nil
}

// performRebalance performs the actual rebalancing process

// performRebalance performs the traditional leave-and-rejoin rebalance
func (gc *GroupConsumer) performRebalance() {
	defer func() {
		gc.mu.Lock()
		<-gc.rebalanceTrigger
		gc.IsRebalancing()
		gc.mu.Unlock()
	}()

	if err := gc.LeaveGroup(); err != nil {
		log.Printf("Failed to leave group for rebalance: %v", err)
		return
	}

	time.Sleep(1 * time.Second)

	if err := gc.JoinGroup(); err != nil {
		log.Printf("Failed to rejoin group after rebalance: %v", err)
		return
	}

	log.Printf("Consumer %s completed traditional rebalance process", gc.ConsumerID)
}

// IsRebalancing returns whether the consumer is currently rebalancing
func (gc *GroupConsumer) IsRebalancing() bool {
	gc.mu.RLock()
	defer gc.mu.RUnlock()
	return gc.rebalancing
}

// WaitForRebalanceComplete waits for any ongoing rebalance to complete
func (gc *GroupConsumer) WaitForRebalanceComplete(timeout time.Duration) error {
	start := time.Now()

	for {
		if !gc.IsRebalancing() {
			return nil
		}

		if time.Since(start) > timeout {
			return fmt.Errorf("timeout waiting for rebalance to complete")
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// ConsumeMessage represents a consumed message
type ConsumeMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// Poll polls for messages from assigned partitions
func (gc *GroupConsumer) Poll(timeout time.Duration) ([]*ConsumeMessage, error) {
	// Check if rebalancing is in progress
	if gc.IsRebalancing() {
		log.Printf("Consumer %s is rebalancing, skipping poll", gc.ConsumerID)
		return []*ConsumeMessage{}, nil
	}

	gc.mu.RLock()
	assignment := gc.assignment
	gc.mu.RUnlock()

	if len(assignment) == 0 {
		return nil, fmt.Errorf("no partitions assigned to this consumer")
	}

	var allMessages []*ConsumeMessage

	for topic, partitions := range assignment {
		for _, partition := range partitions {
			if gc.IsRebalancing() {
				log.Printf("Rebalancing detected during poll, stopping")
				break
			}

			messages, err := gc.pollPartition(topic, partition, timeout)
			if err != nil {
				log.Printf("Failed to poll partition %s-%d: %v", topic, partition, err)
				continue
			}
			allMessages = append(allMessages, messages...)
		}
	}

	return allMessages, nil
}

// pollPartition polls messages from a specific partition
func (gc *GroupConsumer) pollPartition(topic string, partition int32, timeout time.Duration) ([]*ConsumeMessage, error) {
	lastOffset, err := gc.FetchCommittedOffset(topic, partition)
	if err != nil {
		lastOffset = 0
		log.Printf("No committed offset found for %s-%d, starting from offset 0", topic, partition)
	} else {
		lastOffset++
	}

	// Use existing consumer to fetch messages
	consumer := NewConsumer(gc.client)

	fetchResult, err := consumer.Fetch(FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    lastOffset,
		MaxBytes:  1024 * 1024, // 1MB max
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from %s-%d: %w", topic, partition, err)
	}

	if fetchResult.Error != nil {
		return nil, fmt.Errorf("fetch error for %s-%d: %w", topic, partition, fetchResult.Error)
	}

	// Convert to ConsumeMessage format
	var messages []*ConsumeMessage
	for _, msg := range fetchResult.Messages {
		messages = append(messages, &ConsumeMessage{
			Topic:     topic,
			Partition: partition,
			Offset:    msg.Offset,
			Key:       nil, // TODO: Add key support to fetch
			Value:     msg.Value,
			Timestamp: time.Now(), // TODO: Add timestamp support
		})
	}

	return messages, nil
}

// Consume starts consuming messages and calls the handler for each message
func (gc *GroupConsumer) Consume(handler func(*ConsumeMessage) error) error {
	log.Printf("Starting message consumption for group %s, consumer %s", gc.GroupID, gc.ConsumerID)

	for {
		messages, err := gc.Poll(5 * time.Second)
		if err != nil {
			log.Printf("Poll error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, message := range messages {
			if err := handler(message); err != nil {
				log.Printf("Message handler error: %v", err)
				continue
			}

			// Auto-commit offset after successful processing
			if err := gc.CommitOffset(message.Topic, message.Partition, message.Offset, "auto"); err != nil {
				log.Printf("Failed to commit offset: %v", err)
			}
		}

		// Small delay to avoid busy polling
		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// ConsumeWithManualCommit starts consuming with manual offset management
func (gc *GroupConsumer) ConsumeWithManualCommit(handler func(*ConsumeMessage) error) error {
	log.Printf("Starting manual commit consumption for group %s, consumer %s", gc.GroupID, gc.ConsumerID)

	for {
		messages, err := gc.Poll(5 * time.Second)
		if err != nil {
			log.Printf("Poll error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, message := range messages {
			if err := handler(message); err != nil {
				log.Printf("Message handler error: %v", err)
				continue
			}
			// Note: No auto-commit, user must call CommitOffset manually
		}

		// Small delay to avoid busy polling
		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

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
		gc.stopHeartbeat = make(chan struct{}) // Recreate for future use
	}
}

func (gc *GroupConsumer) sendHeartbeat() error {
	requestData, err := gc.buildHeartbeatRequest()
	if err != nil {
		return fmt.Errorf("failed to build heartbeat request: %v", err)
	}

	responseData, err := gc.client.sendMetaRequest(protocol.HeartbeatRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat request: %v", err)
	}

	return gc.parseHeartbeatResponse(responseData)
}

func (gc *GroupConsumer) buildJoinGroupRequest() ([]byte, error) {
	buf := new(bytes.Buffer)

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

	// Handle error cases
	if errorCode != 0 {
		switch errorCode {
		case protocol.ErrorUnknownGroup:
			return fmt.Errorf("unknown consumer group: %s", gc.GroupID)
		case protocol.ErrorUnknownMember:
			return fmt.Errorf("unknown member: %s", gc.ConsumerID)
		case protocol.ErrorRebalanceInProgress:
			log.Printf("Rebalance in progress for group %s", gc.GroupID)
		case protocol.ErrorGenerationMismatch:
			log.Printf("Generation mismatch detected for consumer %s", gc.ConsumerID)
		default:
			return fmt.Errorf("heartbeat failed with error code: %d", errorCode)
		}
	}

	var generation int32
	if err := binary.Read(buf, binary.BigEndian, &generation); err != nil {
		return fmt.Errorf("failed to read generation: %v", err)
	}

	var rebalanceFlag int8
	if err := binary.Read(buf, binary.BigEndian, &rebalanceFlag); err != nil {
		return fmt.Errorf("failed to read rebalance flag: %v", err)
	}

	var memberCount int32
	if err := binary.Read(buf, binary.BigEndian, &memberCount); err != nil {
		return fmt.Errorf("failed to read member count: %v", err)
	}

	var leaderIDLen int16
	if err := binary.Read(buf, binary.BigEndian, &leaderIDLen); err != nil {
		return fmt.Errorf("failed to read leader ID length: %v", err)
	}
	leaderIDBytes := make([]byte, leaderIDLen)
	if _, err := io.ReadFull(buf, leaderIDBytes); err != nil {
		return fmt.Errorf("failed to read leader ID: %v", err)
	}
	leaderID := string(leaderIDBytes)

	if rebalanceFlag == 1 || generation != gc.generation {
		gc.FetchAssignment()
	}

	// 🔥 Update local state
	gc.mu.Lock()
	gc.generation = generation
	gc.leader = leaderID
	gc.mu.Unlock()

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

// FetchAssignment fetches the current assignment from the server without rejoining
func (gc *GroupConsumer) FetchAssignment() error {
	requestData, err := gc.buildFetchAssignmentRequest()
	if err != nil {
		return fmt.Errorf("failed to build fetch assignment request: %v", err)
	}

	responseData, err := gc.client.sendMetaRequest(protocol.FetchAssignmentRequestType, requestData)
	if err != nil {
		return fmt.Errorf("failed to send fetch assignment request: %v", err)
	}

	return gc.parseFetchAssignmentResponse(responseData)
}

func (gc *GroupConsumer) buildFetchAssignmentRequest() ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, int16(1)) // version

	// GroupID
	binary.Write(buf, binary.BigEndian, int16(len(gc.GroupID)))
	buf.WriteString(gc.GroupID)

	// ConsumerID
	binary.Write(buf, binary.BigEndian, int16(len(gc.ConsumerID)))
	buf.WriteString(gc.ConsumerID)

	// Generation
	binary.Write(buf, binary.BigEndian, gc.generation)

	return buf.Bytes(), nil
}

func (gc *GroupConsumer) parseFetchAssignmentResponse(data []byte) error {
	buf := bytes.NewReader(data)

	var errorCode int16
	if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
		return err
	}
	if errorCode != 0 {
		return fmt.Errorf("fetch assignment failed with error code: %d", errorCode)
	}

	var generation int32
	if err := binary.Read(buf, binary.BigEndian, &generation); err != nil {
		return err
	}

	var assignmentCount int32
	if err := binary.Read(buf, binary.BigEndian, &assignmentCount); err != nil {
		return err
	}

	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.generation = generation
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

	log.Printf("Consumer %s updated assignment: %v (generation %d)", gc.ConsumerID, gc.assignment, gc.generation)
	return nil
}
