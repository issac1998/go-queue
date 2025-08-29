package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/statemachine"

	clusterConfig "github.com/issac1998/go-queue/internal/config"
	"github.com/issac1998/go-queue/internal/metadata"
)

const (
	ClusterID = 1 // 固定的集群ID
)

// Manager manages the cluster using Raft
type Manager struct {
	nodeID   uint64
	config   *clusterConfig.ClusterConfig
	nodeHost *dragonboat.NodeHost
	manager  *metadata.Manager

	mu       sync.RWMutex
	isLeader bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewManager creates a new cluster manager
func NewManager(nodeID uint64, clusterConf *clusterConfig.ClusterConfig, manager *metadata.Manager) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cm := &Manager{
		nodeID:  nodeID,
		config:  clusterConf,
		manager: manager,
		ctx:     ctx,
		cancel:  cancel,
	}

	if err := cm.initRaft(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize Raft: %v", err)
	}

	return cm, nil
}

// initRaft initializes the Raft node
func (cm *Manager) initRaft() error {
	// 配置日志
	logger.GetLogger("raft").SetLevel(logger.WARNING)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)

	// 创建NodeHost配置
	nhConfig := config.NodeHostConfig{
		WALDir:         filepath.Join(cm.config.DataDir, "wal"),
		NodeHostDir:    filepath.Join(cm.config.DataDir, "nodehost"),
		RTTMillisecond: cm.config.HeartbeatRTT,
		RaftAddress:    cm.config.RaftAddress,
	}

	// 创建NodeHost
	nodeHost, err := dragonboat.NewNodeHost(nhConfig)
	if err != nil {
		return fmt.Errorf("failed to create NodeHost: %v", err)
	}
	cm.nodeHost = nodeHost

	// 创建Raft配置
	raftConfig := config.Config{
		NodeID:             cm.nodeID,
		ClusterID:          ClusterID,
		ElectionRTT:        cm.config.ElectionRTT,
		HeartbeatRTT:       cm.config.HeartbeatRTT,
		CheckQuorum:        cm.config.CheckQuorum,
		SnapshotEntries:    cm.config.SnapshotEntries,
		CompactionOverhead: cm.config.CompactionOverhead,
	}

	// 解析初始成员
	initialMembers := make(map[uint64]string)
	for i, addr := range cm.config.InitialMembers {
		initialMembers[uint64(i+1)] = addr
	}

	// 启动Raft集群 - 使用v3的方式
	createStateMachine := func(clusterID uint64, nodeID uint64) statemachine.IStateMachine {
		return NewClusterStateMachine(nodeID, cm.manager)
	}

	if err := cm.nodeHost.StartCluster(initialMembers, false, createStateMachine, raftConfig); err != nil {
		return fmt.Errorf("failed to start cluster: %v", err)
	}

	log.Printf("Raft cluster started - NodeID: %d, Address: %s", cm.nodeID, cm.config.RaftAddress)

	// 启动领导者检测
	go cm.leadershipMonitor()

	return nil
}

// leadershipMonitor monitors leadership changes
func (cm *Manager) leadershipMonitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			leaderID, ok, err := cm.nodeHost.GetLeaderID(ClusterID)
			if err != nil {
				log.Printf("Failed to get leader ID: %v", err)
				continue
			}

			cm.mu.Lock()
			wasLeader := cm.isLeader
			cm.isLeader = ok && leaderID == cm.nodeID

			if !wasLeader && cm.isLeader {
				log.Printf("Node %d became leader", cm.nodeID)
				go cm.onBecomeLeader()
			} else if wasLeader && !cm.isLeader {
				log.Printf("Node %d lost leadership", cm.nodeID)
				go cm.onLoseLeader()
			}
			cm.mu.Unlock()
		}
	}
}

// onBecomeLeader handles becoming leader
func (cm *Manager) onBecomeLeader() {
	// 注册自己为broker
	broker := BrokerInfo{
		ID:       cm.nodeID,
		Address:  cm.config.RaftAddress, // 这里应该是业务地址
		RaftAddr: cm.config.RaftAddress,
		Status:   "alive",
	}

	if err := cm.RegisterBroker(&broker); err != nil {
		log.Printf("Failed to register broker: %v", err)
	}
}

// onLoseLeader handles losing leadership
func (cm *Manager) onLoseLeader() {
	log.Printf("Node %d is no longer leader", cm.nodeID)
}

// CreateTopic creates a new topic through Raft
func (cm *Manager) CreateTopic(name string, partitions, replicas int32) (*TopicInfo, error) {
	topicConfig := metadata.TopicConfig{
		Partitions: partitions,
		Replicas:   replicas,
	}

	configData, err := json.Marshal(topicConfig)
	if err != nil {
		return nil, err
	}

	op := Operation{
		Type:  OpCreateTopic,
		Topic: name,
		Data:  configData,
	}

	result, err := cm.propose(&op)
	if err != nil {
		return nil, err
	}

	var topicInfo TopicInfo
	if err := json.Unmarshal(result.Data, &topicInfo); err != nil {
		return nil, err
	}

	return &topicInfo, nil
}

// ProduceMessage produces a message through Raft consensus
func (cm *Manager) ProduceMessage(topic string, partition int32, data []byte) (*ProduceResult, error) {
	if !cm.IsLeader() {
		return nil, fmt.Errorf("not the leader")
	}

	// 创建操作 - 简化，让状态机自己处理offset
	op := &Operation{
		Type:      OpAppendMessage,
		Topic:     topic,
		Partition: partition,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}

	// 序列化操作
	opData, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal operation: %v", err)
	}

	// 通过Raft提交 - 这会自动同步到所有节点
	ctx, cancel := context.WithTimeout(cm.ctx, 10*time.Second)
	defer cancel()

	result, err := cm.nodeHost.SyncPropose(ctx, cm.nodeHost.GetNoOPSession(ClusterID), opData)
	if err != nil {
		return nil, fmt.Errorf("failed to propose message: %v", err)
	}

	// 解析结果
	var produceResult struct {
		MessageID string `json:"message_id"`
		Offset    int64  `json:"offset"`
		Timestamp int64  `json:"timestamp"`
	}

	if err := json.Unmarshal(result.Data, &produceResult); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %v", err)
	}

	return &ProduceResult{
		Topic:     topic,
		Partition: partition,
		Offset:    produceResult.Offset,
		MessageID: produceResult.MessageID,
		Timestamp: produceResult.Timestamp,
	}, nil
}

// RegisterBroker registers a broker in the cluster
func (cm *Manager) RegisterBroker(broker *BrokerInfo) error {
	data, err := json.Marshal(broker)
	if err != nil {
		return err
	}

	op := Operation{
		Type: OpRegisterBroker,
		Data: data,
	}

	_, err = cm.propose(&op)
	return err
}

// GetClusterInfo returns cluster information
func (cm *Manager) GetClusterInfo() (*ClusterInfo, error) {
	// 获取Leader信息
	leaderID, valid, err := cm.nodeHost.GetLeaderID(ClusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get leader ID: %v", err)
	}

	brokers, err := cm.getBrokers()
	if err != nil {
		return nil, err
	}

	topics, err := cm.getTopics()
	if err != nil {
		return nil, err
	}

	return &ClusterInfo{
		LeaderID:    leaderID,
		LeaderValid: valid,
		CurrentNode: cm.nodeID,
		IsLeader:    valid && leaderID == cm.nodeID,
		Brokers:     brokers,
		Topics:      topics,
	}, nil
}

// IsLeader 检查当前节点是否为Leader
func (cm *Manager) IsLeader() bool {
	leaderID, valid, err := cm.nodeHost.GetLeaderID(ClusterID)
	if err != nil || !valid {
		return false
	}
	return leaderID == cm.nodeID
}

// GetLeaderID 获取当前Leader的节点ID
func (cm *Manager) GetLeaderID() (uint64, bool, error) {
	return cm.nodeHost.GetLeaderID(ClusterID)
}

// ReadMessage 从Raft集群读取消息（支持Follower读）
func (cm *Manager) ReadMessage(topic string, partition int32, offset int64, maxBytes int32) ([][]byte, int64, error) {
	// 构建查询请求
	query := struct {
		Type      string `json:"type"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    int64  `json:"offset"`
		MaxBytes  int32  `json:"max_bytes"`
	}{
		Type:      "get_messages",
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		MaxBytes:  maxBytes,
	}

	return cm.readWithReadIndex(query)
}

// readWithReadIndex 使用ReadIndex协议进行强一致性读
func (cm *Manager) readWithReadIndex(query interface{}) ([][]byte, int64, error) {
	ctx, cancel := context.WithTimeout(cm.ctx, 5*time.Second)
	defer cancel()

	// 使用ReadIndex协议确保读取到最新数据
	rs, err := cm.nodeHost.ReadIndex(ClusterID, 3*time.Second)
	if err != nil {
		return nil, 0, fmt.Errorf("ReadIndex failed: %v", err)
	}
	defer rs.Release()

	// 等待ReadIndex完成
	select {
	case result := <-rs.ResultC():
		if !result.Completed() {
			return nil, 0, fmt.Errorf("ReadIndex not completed")
		}

		// 执行本地读取
		readResult, err := cm.nodeHost.ReadLocalNode(rs, query)
		if err != nil {
			return nil, 0, fmt.Errorf("ReadLocalNode failed: %v", err)
		}

		return cm.parseReadResult(readResult)
	case <-ctx.Done():
		return nil, 0, fmt.Errorf("ReadIndex timeout")
	}
}

// parseReadResult 解析读取结果
func (cm *Manager) parseReadResult(result interface{}) ([][]byte, int64, error) {
	// 解析Raft状态机返回的结果
	if resultData, ok := result.([]byte); ok {
		var response struct {
			Messages   [][]byte `json:"messages"`
			NextOffset int64    `json:"next_offset"`
			Error      string   `json:"error"`
		}

		if err := json.Unmarshal(resultData, &response); err != nil {
			return nil, 0, fmt.Errorf("failed to parse read result: %v", err)
		}

		if response.Error != "" {
			return nil, 0, fmt.Errorf("read error: %s", response.Error)
		}

		return response.Messages, response.NextOffset, nil
	}

	return nil, 0, fmt.Errorf("invalid read result format")
}

// propose proposes an operation to the Raft cluster
func (cm *Manager) propose(op *Operation) (*statemachine.Result, error) {
	data, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	session := cm.nodeHost.GetNoOPSession(ClusterID)
	result, err := cm.nodeHost.SyncPropose(ctx, session, data)
	if err != nil {
		return nil, err
	}

	return &statemachine.Result{
		Value: result.Value,
		Data:  result.Data,
	}, nil
}

// query performs a read-only query
func (cm *Manager) query(queryType string) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := cm.nodeHost.SyncRead(ctx, ClusterID, queryType)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Stop stops the cluster manager
func (cm *Manager) Stop() {
	if cm.nodeHost != nil {
		cm.nodeHost.Stop()
	}

	cm.cancel()
}

// 简化的辅助方法
func (cm *Manager) getNextOffset(topic string, partition int32) (int64, error) {
	// 查询当前状态机中的offset信息
	query := struct {
		Type      string `json:"type"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
	}{
		Type:      "get_next_offset",
		Topic:     topic,
		Partition: partition,
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 3*time.Second)
	defer cancel()

	result, err := cm.nodeHost.SyncRead(ctx, ClusterID, queryData)
	if err != nil {
		return 0, err
	}

	var offsetResult struct {
		Offset int64 `json:"offset"`
	}

	if err := json.Unmarshal(result.([]byte), &offsetResult); err != nil {
		return 0, nil // 如果查询失败，返回0
	}

	return offsetResult.Offset, nil
}

// ProduceResult represents the result of a message production
type ProduceResult struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	MessageID string `json:"message_id"`
	Timestamp int64  `json:"timestamp"`
}

// ClusterInfo represents cluster information
type ClusterInfo struct {
	LeaderID    uint64                 `json:"leader_id"`
	LeaderValid bool                   `json:"leader_valid"`
	CurrentNode uint64                 `json:"current_node"`
	IsLeader    bool                   `json:"is_leader"`
	Brokers     map[uint64]*BrokerInfo `json:"brokers"`
	Topics      map[string]*TopicInfo  `json:"topics"`
}

// Consumer Group 相关方法

// JoinConsumerGroup 消费者加入组（集群模式）
func (cm *Manager) JoinConsumerGroup(groupID, consumerID, clientID string, topics []string, sessionTimeout time.Duration) (*metadata.Consumer, error) {
	if !cm.IsLeader() {
		return nil, fmt.Errorf("only leader can handle consumer group operations")
	}

	op := &Operation{
		Type:           OpJoinConsumerGroup,
		GroupID:        groupID,
		ConsumerID:     consumerID,
		ClientID:       clientID,
		Topics:         topics,
		SessionTimeout: sessionTimeout.Milliseconds(),
	}

	data, err := json.Marshal(op)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 5*time.Second)
	defer cancel()

	result, err := cm.nodeHost.SyncPropose(ctx, cm.nodeHost.GetNoOPSession(ClusterID), data)
	if err != nil {
		return nil, err
	}

	// 解析结果
	var response map[string]interface{}
	if err := json.Unmarshal(result.Data, &response); err != nil {
		return nil, err
	}

	// 构建消费者对象返回
	consumer := &metadata.Consumer{
		ID:             consumerID,
		ClientID:       clientID,
		GroupID:        groupID,
		Subscriptions:  topics,
		Assignment:     make(map[string][]int32),
		SessionTimeout: sessionTimeout,
		LastHeartbeat:  time.Now(),
		JoinedAt:       time.Now(),
	}

	return consumer, nil
}

// LeaveConsumerGroup 消费者离开组（集群模式）
func (cm *Manager) LeaveConsumerGroup(groupID, consumerID string) error {
	if !cm.IsLeader() {
		return fmt.Errorf("only leader can handle consumer group operations")
	}

	op := &Operation{
		Type:       OpLeaveConsumerGroup,
		GroupID:    groupID,
		ConsumerID: consumerID,
	}

	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 5*time.Second)
	defer cancel()

	_, err = cm.nodeHost.SyncPropose(ctx, cm.nodeHost.GetNoOPSession(ClusterID), data)
	return err
}

// RebalanceConsumerGroupPartitions 重平衡消费者组分区（集群模式）
func (cm *Manager) RebalanceConsumerGroupPartitions(groupID string, assignment map[string][]int32) error {
	if !cm.IsLeader() {
		return fmt.Errorf("only leader can handle consumer group operations")
	}

	op := &Operation{
		Type:       OpRebalancePartitions,
		GroupID:    groupID,
		Assignment: assignment,
	}

	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 5*time.Second)
	defer cancel()

	_, err = cm.nodeHost.SyncPropose(ctx, cm.nodeHost.GetNoOPSession(ClusterID), data)
	return err
}

// CommitConsumerGroupOffset 提交消费者组offset（集群模式）
func (cm *Manager) CommitConsumerGroupOffset(groupID, topic string, partition int32, offset int64) error {
	op := &Operation{
		Type:      OpCommitOffset,
		GroupID:   groupID,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}

	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 3*time.Second)
	defer cancel()

	_, err = cm.nodeHost.SyncPropose(ctx, cm.nodeHost.GetNoOPSession(ClusterID), data)
	return err
}

// HeartbeatConsumerGroup 消费者组心跳（集群模式）
func (cm *Manager) HeartbeatConsumerGroup(groupID, consumerID string) error {
	op := &Operation{
		Type:       OpHeartbeatConsumer,
		GroupID:    groupID,
		ConsumerID: consumerID,
	}

	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 3*time.Second)
	defer cancel()

	_, err = cm.nodeHost.SyncPropose(ctx, cm.nodeHost.GetNoOPSession(ClusterID), data)
	return err
}

// 简化的辅助方法
func (cm *Manager) getBrokers() (map[uint64]*BrokerInfo, error) {
	brokers := make(map[uint64]*BrokerInfo)

	query := struct {
		Type string `json:"type"`
	}{
		Type: "get_brokers",
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 3*time.Second)
	defer cancel()

	result, err := cm.nodeHost.SyncRead(ctx, ClusterID, queryData)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(result.([]byte), &brokers); err != nil {
		return nil, err
	}

	return brokers, nil
}

func (cm *Manager) getTopics() (map[string]*TopicInfo, error) {
	topics := make(map[string]*TopicInfo)

	query := struct {
		Type string `json:"type"`
	}{
		Type: "get_topics",
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(cm.ctx, 3*time.Second)
	defer cancel()

	result, err := cm.nodeHost.SyncRead(ctx, ClusterID, queryData)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(result.([]byte), &topics); err != nil {
		return nil, err
	}

	return topics, nil
}
