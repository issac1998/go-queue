package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

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
)

// Operation represents a state machine operation
type Operation struct {
	Type      uint32      `json:"type"`
	Topic     string      `json:"topic,omitempty"`
	Partition int32       `json:"partition,omitempty"`
	Data      []byte      `json:"data,omitempty"`
	Metadata  interface{} `json:"metadata,omitempty"`
}

// ClusterStateMachine implements the Raft state machine
type ClusterStateMachine struct {
	nodeID uint64
	mu     sync.RWMutex

	// 集群元数据
	brokers    map[uint64]*BrokerInfo
	topics     map[string]*TopicInfo
	partitions map[string]map[int32]*PartitionInfo

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

// PartitionInfo represents partition information
type PartitionInfo struct {
	Topic    string   `json:"topic"`
	ID       int32    `json:"id"`
	Leader   uint64   `json:"leader"`
	Replicas []uint64 `json:"replicas"`
	ISR      []uint64 `json:"isr"`
}

// NewClusterStateMachine creates a new cluster state machine
func NewClusterStateMachine(nodeID uint64, manager *metadata.Manager) sm.IStateMachine {
	return &ClusterStateMachine{
		nodeID:     nodeID,
		brokers:    make(map[uint64]*BrokerInfo),
		topics:     make(map[string]*TopicInfo),
		partitions: make(map[string]map[int32]*PartitionInfo),
		manager:    manager,
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
		default:
			return nil, fmt.Errorf("unknown query type: %s", q)
		}
	case []byte:
		queryStr := string(q)
		switch queryStr {
		case "brokers":
			return csm.brokers, nil
		case "topics":
			return csm.topics, nil
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
		Brokers    map[uint64]*BrokerInfo              `json:"brokers"`
		Topics     map[string]*TopicInfo               `json:"topics"`
		Partitions map[string]map[int32]*PartitionInfo `json:"partitions"`
	}{
		Brokers:    csm.brokers,
		Topics:     csm.topics,
		Partitions: csm.partitions,
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
		Brokers    map[uint64]*BrokerInfo              `json:"brokers"`
		Topics     map[string]*TopicInfo               `json:"topics"`
		Partitions map[string]map[int32]*PartitionInfo `json:"partitions"`
	}

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	csm.brokers = snapshot.Brokers
	csm.topics = snapshot.Topics
	csm.partitions = snapshot.Partitions

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

	// 创建分区信息
	csm.partitions[op.Topic] = make(map[int32]*PartitionInfo)
	brokerList := csm.getAliveBrokers()

	for i := int32(0); i < topicConfig.Partitions; i++ {
		replicas := csm.assignReplicas(brokerList, int(topicConfig.Replicas))
		partitionInfo := &PartitionInfo{
			Topic:    op.Topic,
			ID:       i,
			Leader:   replicas[0], // 第一个副本作为Leader
			Replicas: replicas,
			ISR:      replicas, // 初始时所有副本都在ISR中
		}
		csm.partitions[op.Topic][i] = partitionInfo
	}

	// 在本地Manager中创建topic
	if csm.manager != nil {
		_, err := csm.manager.CreateTopic(op.Topic, &topicConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic in local manager: %v", err)
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

	return []byte("OK"), nil
}

// appendMessage appends a message to a partition
func (csm *ClusterStateMachine) appendMessage(op *Operation) ([]byte, error) {
	partitionMap, exists := csm.partitions[op.Topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", op.Topic)
	}

	partitionInfo, exists := partitionMap[op.Partition]
	if !exists {
		return nil, fmt.Errorf("partition %d not found for topic %s", op.Partition, op.Topic)
	}

	// 检查当前节点是否为该分区的Leader
	if partitionInfo.Leader != csm.nodeID {
		return nil, fmt.Errorf("not the leader for partition %d of topic %s", op.Partition, op.Topic)
	}

	// 在本地Manager中写入消息
	if csm.manager != nil {
		offset, err := csm.manager.WriteMessage(op.Topic, op.Partition, op.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to write message: %v", err)
		}

		result := struct {
			Offset int64 `json:"offset"`
		}{
			Offset: offset,
		}
		resultBytes, _ := json.Marshal(result)
		return resultBytes, nil
	}

	return []byte("OK"), nil
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
