package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/metadata"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

// 操作类型
const (
	OpCreateTopic      = 1
	OpDeleteTopic      = 2
	OpAppendMessage    = 3
	OpUpdateMetadata   = 4
	OpRegisterBroker   = 5
	OpUnregisterBroker = 6

	// 消费者组操作
	OpJoinConsumerGroup   = 7
	OpLeaveConsumerGroup  = 8
	OpRebalancePartitions = 9
	OpCommitOffset        = 10
	OpHeartbeatConsumer   = 11
)

// Operation represents a state machine operation
type Operation struct {
	Type      uint32      `json:"type"`
	Topic     string      `json:"topic,omitempty"`
	Partition int32       `json:"partition,omitempty"`
	Data      []byte      `json:"data,omitempty"`
	Metadata  interface{} `json:"metadata,omitempty"`

	// 消费者组相关字段
	GroupID        string             `json:"group_id,omitempty"`
	ConsumerID     string             `json:"consumer_id,omitempty"`
	ClientID       string             `json:"client_id,omitempty"`
	Topics         []string           `json:"topics,omitempty"`
	SessionTimeout int64              `json:"session_timeout,omitempty"` // 毫秒
	Offset         int64              `json:"offset,omitempty"`
	Assignment     map[string][]int32 `json:"assignment,omitempty"`
	Timestamp int64 `json:"timestamp,omitempty"`
}

// MessageEntry represents a message in the Raft log
type MessageEntry struct {
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Data      []byte `json:"data"`
	Timestamp int64  `json:"timestamp"`
	Checksum  uint32 `json:"checksum"`
}

// PartitionInfo represents partition information
type PartitionInfo struct {
	Topic string `json:"topic"`
	ID    int32  `json:"id"`
	// 移除 Leader, ISR 概念 - Raft自动管理Leader
	Replicas []uint64 `json:"replicas"`
}

// ClusterStateMachine implements the Raft state machine
type ClusterStateMachine struct {
	nodeID uint64
	mu     sync.RWMutex

	// 集群元数据
	brokers    map[uint64]*BrokerInfo
	topics     map[string]*TopicInfo
	partitions map[string]map[int32]*PartitionInfo

	// 消费者组数据
	consumerGroups map[string]*metadata.ConsumerGroup
	groupOffsets   map[string]map[string]map[int32]*metadata.OffsetCommit // groupID -> topic -> partition -> offset
	// 消息数据 - 存储在Raft状态机中，自动同步到所有节点
	messages map[string]map[int32][]*MessageEntry // topic -> partition -> messages
	offsets  map[string]map[int32]int64           // topic -> partition -> nextOffset

	// 本地数据管理器
	manager *metadata.Manager
}

// BrokerInfo represents broker information
type BrokerInfo struct {
	ID       uint64 `json:"id"`
	Address  string `json:"address"`
	RaftAddr string `json:"raft_addr"`
	Status   string `json:"status"`
}

// TopicInfo represents topic information
type TopicInfo struct {
	Name       string `json:"name"`
	Partitions int32  `json:"partitions"`
	Replicas   int32  `json:"replicas"`
}

// NewClusterStateMachine creates a new cluster state machine
func NewClusterStateMachine(nodeID uint64, manager *metadata.Manager) sm.IStateMachine {
	return &ClusterStateMachine{
		nodeID:         nodeID,
		brokers:        make(map[uint64]*BrokerInfo),
		topics:         make(map[string]*TopicInfo),
		partitions:     make(map[string]map[int32]*PartitionInfo),
		consumerGroups: make(map[string]*metadata.ConsumerGroup),
		groupOffsets:   make(map[string]map[string]map[int32]*metadata.OffsetCommit),
		manager:        manager,
		messages:       make(map[string]map[int32][]*MessageEntry),
		offsets:        make(map[string]map[int32]int64),
	}
}

// Update updates the state machine (dragonboat v3 interface)
func (csm *ClusterStateMachine) Update(data []byte) (sm.Result, error) {
	var op Operation
	if err := json.Unmarshal(data, &op); err != nil {
		return sm.Result{Value: 0}, err
	}

	result, err := csm.applyOperation(&op)
	if err != nil {
		return sm.Result{Value: 0}, err
	}

	// 返回成功结果
	return sm.Result{
		Value: 1,
		Data:  result,
	}, nil
}

// Lookup performs a read-only query (dragonboat v3 interface)
func (csm *ClusterStateMachine) Lookup(query interface{}) (interface{}, error) {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	switch q := query.(type) {
	case string:
		switch q {
		case "brokers":
			return csm.brokers, nil
		case "topics":
			return csm.topics, nil
		case "partitions":
			return csm.partitions, nil
		case "replication_log":
			// 返回消息数据，而不是日志结构
			var allMessages []*MessageEntry
			for _, partitionMap := range csm.messages {
				for _, messages := range partitionMap {
					allMessages = append(allMessages, messages...)
				}
			}
			return allMessages, nil
		default:
			return nil, fmt.Errorf("unknown query type: %s", q)
		}
	case []byte:
		// 尝试解析JSON查询
		var jsonQuery struct {
			Type      string `json:"type"`
			Topic     string `json:"topic,omitempty"`
			Partition int32  `json:"partition,omitempty"`
			Offset    int64  `json:"offset,omitempty"`
			MaxBytes  int32  `json:"max_bytes,omitempty"`
		}

		if err := json.Unmarshal(q, &jsonQuery); err == nil {
			switch jsonQuery.Type {
			case "get_next_offset":
				return csm.getNextOffsetQuery(jsonQuery.Topic, jsonQuery.Partition)
			case "get_partition_info":
				return csm.getPartitionInfo(jsonQuery.Topic, jsonQuery.Partition)
			case "get_messages":
				return csm.getMessagesWithRange(jsonQuery.Topic, jsonQuery.Partition, jsonQuery.Offset, jsonQuery.MaxBytes)
			case "get_brokers":
				return csm.brokers, nil
			case "get_topics":
				return csm.topics, nil
			}
		}

		// 兼容原有的字符串查询
		queryStr := string(q)
		switch queryStr {
		case "brokers":
			return csm.brokers, nil
		case "topics":
			return csm.topics, nil
		case "partitions":
			return csm.partitions, nil
		case "replication_log":
			// 返回消息数据，而不是日志结构
			var allMessages []*MessageEntry
			for _, partitionMap := range csm.messages {
				for _, messages := range partitionMap {
					allMessages = append(allMessages, messages...)
				}
			}
			return allMessages, nil
		default:
			return nil, fmt.Errorf("unknown query type: %s", queryStr)
		}
	default:
		return nil, fmt.Errorf("unsupported query type")
	}
}

// SaveSnapshot saves the current state as a snapshot (dragonboat v3 interface)
func (csm *ClusterStateMachine) SaveSnapshot(w io.Writer, fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	snapshot := struct {
		Brokers        map[uint64]*BrokerInfo                                 `json:"brokers"`
		Topics         map[string]*TopicInfo                                  `json:"topics"`
		Partitions     map[string]map[int32]*PartitionInfo                    `json:"partitions"`
		ConsumerGroups map[string]*metadata.ConsumerGroup                     `json:"consumer_groups"`
		GroupOffsets   map[string]map[string]map[int32]*metadata.OffsetCommit `json:"group_offsets"`
		Messages       map[string]map[int32][]*MessageEntry                   `json:"messages"`
		Offsets        map[string]map[int32]int64                             `json:"offsets"`
	}{
		Brokers:        csm.brokers,
		Topics:         csm.topics,
		Partitions:     csm.partitions,
		ConsumerGroups: csm.consumerGroups,
		GroupOffsets:   csm.groupOffsets,
		Messages:       csm.messages,
		Offsets:        csm.offsets,
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// RecoverFromSnapshot recovers state from a snapshot (dragonboat v3 interface)
func (csm *ClusterStateMachine) RecoverFromSnapshot(r io.Reader, files []sm.SnapshotFile, done <-chan struct{}) error {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	var snapshot struct {
		Brokers        map[uint64]*BrokerInfo                                 `json:"brokers"`
		Topics         map[string]*TopicInfo                                  `json:"topics"`
		Partitions     map[string]map[int32]*PartitionInfo                    `json:"partitions"`
		ConsumerGroups map[string]*metadata.ConsumerGroup                     `json:"consumer_groups"`
		GroupOffsets   map[string]map[string]map[int32]*metadata.OffsetCommit `json:"group_offsets"`

		Messages map[string]map[int32][]*MessageEntry `json:"messages"`
		Offsets  map[string]map[int32]int64           `json:"offsets"`
	}

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	csm.brokers = snapshot.Brokers
	csm.topics = snapshot.Topics
	csm.partitions = snapshot.Partitions
	csm.messages = snapshot.Messages
	csm.offsets = snapshot.Offsets

	// 恢复消费者组数据
	if snapshot.ConsumerGroups != nil {
		csm.consumerGroups = snapshot.ConsumerGroups
	} else {
		csm.consumerGroups = make(map[string]*metadata.ConsumerGroup)
	}

	if snapshot.GroupOffsets != nil {
		csm.groupOffsets = snapshot.GroupOffsets
	} else {
		csm.groupOffsets = make(map[string]map[string]map[int32]*metadata.OffsetCommit)
	}

	return nil
}

// Close closes the state machine (dragonboat v3 interface)
func (csm *ClusterStateMachine) Close() error {
	// 清理资源
	return nil
}

// GetHash returns a hash of the current state (dragonboat v3 interface)
func (csm *ClusterStateMachine) GetHash() uint64 {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	// 简化的哈希实现，实际应该更复杂
	return uint64(len(csm.brokers) + len(csm.topics))
}

// applyOperation applies an operation to the state machine
func (csm *ClusterStateMachine) applyOperation(op *Operation) ([]byte, error) {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	switch op.Type {
	case OpCreateTopic:
		return csm.createTopic(op)
	case OpDeleteTopic:
		return csm.deleteTopic(op)
	case OpAppendMessage:
		return csm.appendMessage(op)
	case OpUpdateMetadata:
		return csm.updateMetadata(op)
	case OpRegisterBroker:
		return csm.registerBroker(op)
	case OpUnregisterBroker:
		return csm.unregisterBroker(op)
	case OpJoinConsumerGroup:
		return csm.joinConsumerGroup(op)
	case OpLeaveConsumerGroup:
		return csm.leaveConsumerGroup(op)
	case OpRebalancePartitions:
		return csm.rebalancePartitions(op)
	case OpCommitOffset:
		return csm.commitOffset(op)
	case OpHeartbeatConsumer:
		return csm.heartbeatConsumer(op)
	default:
		return nil, fmt.Errorf("unknown operation type: %d", op.Type)
	}
}

// createTopic creates a new topic
func (csm *ClusterStateMachine) createTopic(op *Operation) ([]byte, error) {
	var topicConfig metadata.TopicConfig
	if err := json.Unmarshal(op.Data, &topicConfig); err != nil {
		return nil, err
	}

	// 检查topic是否已存在
	if _, exists := csm.topics[op.Topic]; exists {
		return nil, fmt.Errorf("topic %s already exists", op.Topic)
	}

	// 创建topic信息
	topicInfo := &TopicInfo{
		Name:       op.Topic,
		Partitions: topicConfig.Partitions,
		Replicas:   topicConfig.Replicas,
	}
	csm.topics[op.Topic] = topicInfo

	// 创建分区信息（简化 - 不需要Leader/ISR概念）
	csm.partitions[op.Topic] = make(map[int32]*PartitionInfo)
	brokerList := csm.getAliveBrokers()

	for i := int32(0); i < topicConfig.Partitions; i++ {
		replicas := csm.assignReplicas(brokerList, int(topicConfig.Replicas))
		partitionInfo := &PartitionInfo{
			Topic:    op.Topic,
			ID:       i,
			Replicas: replicas,
		}
		csm.partitions[op.Topic][i] = partitionInfo
	}

	// 初始化消息存储
	csm.messages[op.Topic] = make(map[int32][]*MessageEntry)
	csm.offsets[op.Topic] = make(map[int32]int64)
	for i := int32(0); i < topicConfig.Partitions; i++ {
		csm.messages[op.Topic][i] = make([]*MessageEntry, 0)
		csm.offsets[op.Topic][i] = 0
	}

	// 在本地Manager中创建topic
	if csm.manager != nil {
		_, err := csm.manager.CreateTopic(op.Topic, &topicConfig)
		if err != nil {
			log.Printf("Warning: failed to create topic in local manager: %v", err)
		}
	}

	result, _ := json.Marshal(topicInfo)
	return result, nil
}

// deleteTopic deletes a topic
func (csm *ClusterStateMachine) deleteTopic(op *Operation) ([]byte, error) {
	if _, exists := csm.topics[op.Topic]; !exists {
		return nil, fmt.Errorf("topic %s not found", op.Topic)
	}

	delete(csm.topics, op.Topic)
	delete(csm.partitions, op.Topic)
	delete(csm.messages, op.Topic)
	delete(csm.offsets, op.Topic)

	return []byte("OK"), nil
}

// appendMessage appends a message through Raft (自动同步到所有节点)
func (csm *ClusterStateMachine) appendMessage(op *Operation) ([]byte, error) {
	// 检查topic和partition是否存在
	partitionMap, exists := csm.partitions[op.Topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", op.Topic)
	}

	_, exists = partitionMap[op.Partition]
	if !exists {
		return nil, fmt.Errorf("partition %d not found for topic %s", op.Partition, op.Topic)
	}

	// 获取下一个offset
	if csm.offsets[op.Topic] == nil {
		csm.offsets[op.Topic] = make(map[int32]int64)
	}
	nextOffset := csm.offsets[op.Topic][op.Partition]

	// 创建消息条目
	messageID := fmt.Sprintf("%s-%d-%d-%d", op.Topic, op.Partition, nextOffset, time.Now().UnixNano())
	timestamp := time.Now().UnixMilli()

	messageEntry := &MessageEntry{
		ID:        messageID,
		Topic:     op.Topic,
		Partition: op.Partition,
		Offset:    nextOffset,
		Data:      op.Data,
		Timestamp: timestamp,
		Checksum:  csm.calculateChecksum(op.Data),
	}

	// 存储消息到状态机 - 这会自动通过Raft同步到所有节点
	if csm.messages[op.Topic] == nil {
		csm.messages[op.Topic] = make(map[int32][]*MessageEntry)
	}
	csm.messages[op.Topic][op.Partition] = append(csm.messages[op.Topic][op.Partition], messageEntry)

	// 更新offset
	csm.offsets[op.Topic][op.Partition] = nextOffset + 1

	// 在本地存储中写入消息（每个节点都会执行这个操作）
	if csm.manager != nil {
		_, err := csm.manager.WriteMessageDirect(op.Topic, op.Partition, op.Data, nextOffset)
		if err != nil {
			log.Printf("Warning: failed to write message to local storage: %v", err)
			// 不返回错误，因为数据已经在Raft状态机中了
		}
	}

	// 返回结果
	result := struct {
		MessageID string `json:"message_id"`
		Offset    int64  `json:"offset"`
		Timestamp int64  `json:"timestamp"`
	}{
		MessageID: messageID,
		Offset:    nextOffset,
		Timestamp: timestamp,
	}

	resultBytes, _ := json.Marshal(result)
	return resultBytes, nil
}

// updateMetadata updates cluster metadata
func (csm *ClusterStateMachine) updateMetadata(op *Operation) ([]byte, error) {
	// 处理元数据更新逻辑
	return []byte("OK"), nil
}

// registerBroker registers a new broker
func (csm *ClusterStateMachine) registerBroker(op *Operation) ([]byte, error) {
	var broker BrokerInfo
	if err := json.Unmarshal(op.Data, &broker); err != nil {
		return nil, err
	}

	csm.brokers[broker.ID] = &broker
	return []byte("OK"), nil
}

// unregisterBroker unregisters a broker
func (csm *ClusterStateMachine) unregisterBroker(op *Operation) ([]byte, error) {
	var brokerID uint64
	if err := json.Unmarshal(op.Data, &brokerID); err != nil {
		return nil, err
	}

	delete(csm.brokers, brokerID)
	return []byte("OK"), nil
}

// getAliveBrokers returns list of alive broker IDs
func (csm *ClusterStateMachine) getAliveBrokers() []uint64 {
	var brokers []uint64
	for id, broker := range csm.brokers {
		if broker.Status == "alive" {
			brokers = append(brokers, id)
		}
	}
	return brokers
}

// assignReplicas assigns replicas for a partition
func (csm *ClusterStateMachine) assignReplicas(brokers []uint64, replicaCount int) []uint64 {
	if len(brokers) == 0 {
		return []uint64{csm.nodeID} // 如果没有其他broker，使用自己
	}

	if replicaCount > len(brokers) {
		replicaCount = len(brokers)
	}

	replicas := make([]uint64, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicas[i] = brokers[i%len(brokers)]
	}

	return replicas
}

// Consumer Group 相关方法

// joinConsumerGroup 处理消费者加入组的操作
func (csm *ClusterStateMachine) joinConsumerGroup(op *Operation) ([]byte, error) {
	// 获取或创建消费者组
	group, exists := csm.consumerGroups[op.GroupID]
	if !exists {
		group = &metadata.ConsumerGroup{
			ID:         op.GroupID,
			Members:    make(map[string]*metadata.Consumer),
			Partitions: make(map[string][]int32),
			State:      metadata.GroupStateEmpty,
			Generation: 0,
		}
		csm.consumerGroups[op.GroupID] = group
	}

	// 创建消费者
	consumer := &metadata.Consumer{
		ID:             op.ConsumerID,
		ClientID:       op.ClientID,
		GroupID:        op.GroupID,
		Subscriptions:  op.Topics,
		Assignment:     make(map[string][]int32),
		SessionTimeout: time.Duration(op.SessionTimeout) * time.Millisecond,
	}

	// 添加到组中
	group.Members[op.ConsumerID] = consumer

	// 如果是第一个成员，设为Leader
	if len(group.Members) == 1 {
		group.Leader = op.ConsumerID
	}

	// 触发重平衡
	if group.State == metadata.GroupStateStable {
		group.State = metadata.GroupStatePreparingRebalance
		group.Generation++
	} else if group.State == metadata.GroupStateEmpty {
		group.State = metadata.GroupStatePreparingRebalance
		group.Generation = 1
	}

	// 返回消费者信息
	result := map[string]interface{}{
		"consumer_id": consumer.ID,
		"group_id":    group.ID,
		"generation":  group.Generation,
		"leader":      group.Leader,
		"state":       group.State,
	}

	data, err := json.Marshal(result)
	return data, err
}

// leaveConsumerGroup 处理消费者离开组的操作
func (csm *ClusterStateMachine) leaveConsumerGroup(op *Operation) ([]byte, error) {
	group, exists := csm.consumerGroups[op.GroupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", op.GroupID)
	}

	// 移除消费者
	delete(group.Members, op.ConsumerID)

	// 如果是Leader离开且还有其他成员，选择新Leader
	if group.Leader == op.ConsumerID && len(group.Members) > 0 {
		for memberID := range group.Members {
			group.Leader = memberID
			break
		}
	}

	// 更新组状态
	if len(group.Members) == 0 {
		group.State = metadata.GroupStateEmpty
	} else {
		group.State = metadata.GroupStatePreparingRebalance
		group.Generation++
	}

	result := map[string]interface{}{
		"success":     true,
		"group_state": group.State,
		"generation":  group.Generation,
	}

	data, err := json.Marshal(result)
	return data, err
}

// rebalancePartitions 处理分区重平衡操作
func (csm *ClusterStateMachine) rebalancePartitions(op *Operation) ([]byte, error) {
	group, exists := csm.consumerGroups[op.GroupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", op.GroupID)
	}

	if group.State != metadata.GroupStatePreparingRebalance {
		return nil, fmt.Errorf("group %s is not in preparing rebalance state", op.GroupID)
	}

	// 使用传入的分区分配结果
	if op.Assignment != nil {
		// 更新每个消费者的分配
		for consumerID := range group.Members {
			if assignment, exists := op.Assignment[consumerID]; exists {
				if consumer, exists := group.Members[consumerID]; exists {
					consumer.Assignment = map[string][]int32{op.Topic: assignment}
				}
			}
		}
	}

	// 更新组状态
	group.State = metadata.GroupStateStable

	result := map[string]interface{}{
		"success":     true,
		"generation":  group.Generation,
		"assignments": op.Assignment,
	}

	data, err := json.Marshal(result)
	return data, err
}

// commitOffset 处理offset提交操作
func (csm *ClusterStateMachine) commitOffset(op *Operation) ([]byte, error) {
	// 初始化offset存储结构
	if csm.groupOffsets[op.GroupID] == nil {
		csm.groupOffsets[op.GroupID] = make(map[string]map[int32]*metadata.OffsetCommit)
	}
	if csm.groupOffsets[op.GroupID][op.Topic] == nil {
		csm.groupOffsets[op.GroupID][op.Topic] = make(map[int32]*metadata.OffsetCommit)
	}

	// 提交offset
	offsetCommit := &metadata.OffsetCommit{
		Topic:     op.Topic,
		Partition: op.Partition,
		Offset:    op.Offset,
		Metadata:  "", // 可以从op.Metadata获取
	}

	csm.groupOffsets[op.GroupID][op.Topic][op.Partition] = offsetCommit

	result := map[string]interface{}{
		"success": true,
		"offset":  op.Offset,
	}

	data, err := json.Marshal(result)
	return data, err
}

// heartbeatConsumer 处理消费者心跳操作
func (csm *ClusterStateMachine) heartbeatConsumer(op *Operation) ([]byte, error) {
	group, exists := csm.consumerGroups[op.GroupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", op.GroupID)
	}

	consumer, exists := group.Members[op.ConsumerID]
	if !exists {
		return nil, fmt.Errorf("consumer %s not found in group %s", op.ConsumerID, op.GroupID)
	}

	// 更新心跳时间
	consumer.LastHeartbeat = time.Now()

	result := map[string]interface{}{
		"success":    true,
		"state":      group.State,
		"generation": group.Generation,
	}

	data, err := json.Marshal(result)
	return data, err
}

// 辅助方法
func (csm *ClusterStateMachine) calculateChecksum(data []byte) uint32 {
	// 简单的校验和计算
	var sum uint32
	for _, b := range data {
		sum += uint32(b)
	}
	return sum
}

// 查询辅助方法
func (csm *ClusterStateMachine) getNextOffsetQuery(topic string, partition int32) (interface{}, error) {
	if csm.offsets[topic] != nil {
		if offset, exists := csm.offsets[topic][partition]; exists {
			return map[string]interface{}{"offset": offset}, nil
		}
	}

	// 如果没有记录，返回0
	return map[string]interface{}{"offset": int64(0)}, nil
}

func (csm *ClusterStateMachine) getPartitionInfo(topic string, partition int32) (interface{}, error) {
	if partitionMap, exists := csm.partitions[topic]; exists {
		if partitionInfo, exists := partitionMap[partition]; exists {
			return partitionInfo, nil
		}
	}
	return nil, fmt.Errorf("partition %d not found for topic %s", partition, topic)
}

func (csm *ClusterStateMachine) getMessages(topic string, partition int32) (interface{}, error) {
	if partitionMap, exists := csm.messages[topic]; exists {
		if partitionMessages, exists := partitionMap[partition]; exists {
			return partitionMessages, nil
		}
	}
	return nil, fmt.Errorf("messages for partition %d not found for topic %s", partition, topic)
}

// getMessagesWithRange 根据offset和maxBytes获取消息
func (csm *ClusterStateMachine) getMessagesWithRange(topic string, partition int32, offset int64, maxBytes int32) (interface{}, error) {
	if partitionMap, exists := csm.messages[topic]; exists {
		if partitionMessages, exists := partitionMap[partition]; exists {
			// 过滤出指定offset范围的消息
			var resultMessages [][]byte
			var totalBytes int32 = 0
			nextOffset := offset

			for _, msg := range partitionMessages {
				if msg.Offset >= offset {
					if maxBytes > 0 && totalBytes+int32(len(msg.Data)) > maxBytes {
						break
					}
					resultMessages = append(resultMessages, msg.Data)
					totalBytes += int32(len(msg.Data))
					nextOffset = msg.Offset + 1
				}
			}

			// 返回JSON格式的响应
			response := struct {
				Messages   [][]byte `json:"messages"`
				NextOffset int64    `json:"next_offset"`
				Error      string   `json:"error"`
			}{
				Messages:   resultMessages,
				NextOffset: nextOffset,
				Error:      "",
			}

			responseData, err := json.Marshal(response)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal response: %v", err)
			}

			return responseData, nil
		}
	}

	// 如果topic/partition不存在，返回空结果
	response := struct {
		Messages   [][]byte `json:"messages"`
		NextOffset int64    `json:"next_offset"`
		Error      string   `json:"error"`
	}{
		Messages:   [][]byte{},
		NextOffset: offset,
		Error:      "",
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal empty response: %v", err)
	}

	return responseData, nil
}
