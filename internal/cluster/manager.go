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

// ProduceMessage produces a message through Raft
func (cm *Manager) ProduceMessage(topic string, partition int32, data []byte) error {
	op := Operation{
		Type:      OpAppendMessage,
		Topic:     topic,
		Partition: partition,
		Data:      data,
	}

	_, err := cm.propose(&op)
	return err
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

// GetClusterInfo gets cluster information
func (cm *Manager) GetClusterInfo() (*ClusterInfo, error) {
	// 查询brokers
	brokers, err := cm.query("brokers")
	if err != nil {
		return nil, err
	}

	// 查询topics
	topics, err := cm.query("topics")
	if err != nil {
		return nil, err
	}

	clusterInfo := &ClusterInfo{
		Brokers: brokers.(map[uint64]*BrokerInfo),
		Topics:  topics.(map[string]*TopicInfo),
	}

	return clusterInfo, nil
}

// IsLeader returns whether current node is leader
func (cm *Manager) IsLeader() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.isLeader
}

// GetLeaderID returns the current leader ID
func (cm *Manager) GetLeaderID() (uint64, bool, error) {
	return cm.nodeHost.GetLeaderID(ClusterID)
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
func (cm *Manager) Stop() error {
	cm.cancel()

	if cm.nodeHost != nil {
		cm.nodeHost.Stop()
	}

	return nil
}

// ClusterInfo represents cluster information
type ClusterInfo struct {
	Brokers map[uint64]*BrokerInfo `json:"brokers"`
	Topics  map[string]*TopicInfo  `json:"topics"`
}
