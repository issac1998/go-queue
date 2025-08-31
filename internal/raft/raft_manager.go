package raft

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/statemachine"
)

// RaftConfig contains Dragonboat-specific configuration
type RaftConfig struct {
	RTTMillisecond     uint64 `yaml:"rtt_millisecond"`
	HeartbeatRTT       uint64 `yaml:"heartbeat_rtt"`
	ElectionRTT        uint64 `yaml:"election_rtt"`
	CheckQuorum        bool   `yaml:"check_quorum"`
	SnapshotEntries    uint64 `yaml:"snapshot_entries"`
	CompactionOverhead uint64 `yaml:"compaction_overhead"`
	MaxInMemLogSize    uint64 `yaml:"max_in_mem_log_size"`

	ControllerGroupID      uint64 `yaml:"controller_group_id"`
	ControllerSnapshotFreq uint64 `yaml:"controller_snapshot_freq"`

	NodeID    uint64 `yaml:"node_id"`
	RaftAddr  string `yaml:"raft_addr"`
	MutualTLS bool   `yaml:"mutual_tls"`
	CAFile    string `yaml:"ca_file"`
	CertFile  string `yaml:"cert_file"`
	KeyFile   string `yaml:"key_file"`
}

// RaftManager manages all Raft Groups for a broker
type RaftManager struct {
	config   *RaftConfig
	dataDir  string
	nodeHost *dragonboat.NodeHost

	groups map[uint64]*RaftGroup
	mu     sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

// RaftGroup represents an active Raft group
type RaftGroup struct {
	// Groups或许需要获取Leader接口，用作后续从Leader读Partition
	GroupID      uint64
	Members      map[uint64]string // NodeID -> Address
	StateMachine statemachine.IStateMachine
	IsController bool
}

// NewRaftManager creates a new RaftManager instance with real Dragonboat integration
func NewRaftManager(raftConfig *RaftConfig, dataDir string) (*RaftManager, error) {
	if raftConfig == nil {
		raftConfig = &RaftConfig{
			RTTMillisecond:         200,
			HeartbeatRTT:           5,
			ElectionRTT:            10,
			CheckQuorum:            true,
			SnapshotEntries:        10000,
			CompactionOverhead:     5000,
			MaxInMemLogSize:        67108864,
			ControllerGroupID:      1,
			ControllerSnapshotFreq: 1000,
			NodeID:                 1,
			RaftAddr:               "localhost:63001",
			MutualTLS:              false,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	rm := &RaftManager{
		config:  raftConfig,
		dataDir: dataDir,
		groups:  make(map[uint64]*RaftGroup),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Initialize Dragonboat NodeHost
	if err := rm.initNodeHost(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize NodeHost: %v", err)
	}

	return rm, nil
}

// initNodeHost initializes the Dragonboat NodeHost
func (rm *RaftManager) initNodeHost() error {
	nhConfig := config.NodeHostConfig{
		WALDir:         filepath.Join(rm.dataDir, "wal"),
		NodeHostDir:    filepath.Join(rm.dataDir, "nodehost"),
		RTTMillisecond: rm.config.RTTMillisecond,
		RaftAddress:    rm.config.RaftAddr,
		MutualTLS:      rm.config.MutualTLS,
		CAFile:         rm.config.CAFile,
		CertFile:       rm.config.CertFile,
		KeyFile:        rm.config.KeyFile,
	}

	nodeHost, err := dragonboat.NewNodeHost(nhConfig)
	if err != nil {
		return fmt.Errorf("failed to create NodeHost: %v", err)
	}

	rm.nodeHost = nodeHost
	log.Printf("Dragonboat NodeHost initialized successfully on %s", rm.config.RaftAddr)

	return nil
}

// StartRaftGroup starts a new Raft group with the specified configuration
func (rm *RaftManager) StartRaftGroup(groupID uint64, members map[uint64]string, sm statemachine.IStateMachine, join bool) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.groups[groupID]; exists {
		return fmt.Errorf("raft group %d already exists", groupID)
	}

	raftConfig := config.Config{
		NodeID:             rm.config.NodeID,
		ClusterID:          groupID,
		ElectionRTT:        rm.config.ElectionRTT,
		HeartbeatRTT:       rm.config.HeartbeatRTT,
		CheckQuorum:        rm.config.CheckQuorum,
		SnapshotEntries:    rm.config.SnapshotEntries,
		CompactionOverhead: rm.config.CompactionOverhead,
	}

	err := rm.nodeHost.StartCluster(members, join, func(uint64, uint64) statemachine.IStateMachine {
		return sm
	}, raftConfig)

	if err != nil {
		return fmt.Errorf("failed to start raft group %d: %v", groupID, err)
	}

	rm.groups[groupID] = &RaftGroup{
		GroupID:      groupID,
		Members:      members,
		StateMachine: sm,
		IsController: groupID == rm.config.ControllerGroupID,
	}

	log.Printf("Raft group %d started successfully", groupID)
	return nil
}

// HasRaftGroup checks if a Raft group exists
func (rm *RaftManager) HasRaftGroup(groupID uint64) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	_, exists := rm.groups[groupID]
	return exists
}

// StopRaftGroup stops a Raft group
func (rm *RaftManager) StopRaftGroup(groupID uint64) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.groups[groupID]; !exists {
		return fmt.Errorf("raft group %d not found", groupID)
	}

	if err := rm.nodeHost.StopCluster(groupID); err != nil {
		return fmt.Errorf("failed to stop raft group %d: %v", groupID, err)
	}

	delete(rm.groups, groupID)
	log.Printf("Raft group %d stopped successfully", groupID)
	return nil
}

// SyncPropose submits a proposal to the specified Raft group and waits for result
func (rm *RaftManager) SyncPropose(ctx context.Context, groupID uint64, data []byte) (statemachine.Result, error) {
	session := rm.nodeHost.GetNoOPSession(groupID)

	result, err := rm.nodeHost.SyncPropose(ctx, session, data)
	if err != nil {
		return statemachine.Result{}, fmt.Errorf("sync propose failed for group %d: %v", groupID, err)
	}

	return result, nil
}

// SyncRead performs a linearizable read from the specified Raft group
func (rm *RaftManager) SyncRead(ctx context.Context, groupID uint64, query []byte) (interface{}, error) {
	result, err := rm.nodeHost.SyncRead(ctx, groupID, query)
	if err != nil {
		return nil, fmt.Errorf("sync read failed for group %d: %v", groupID, err)
	}

	return result, nil
}

// TransferLeadership transfers leadership of the specified group to target node
func (rm *RaftManager) TransferLeadership(groupID uint64, targetNodeID uint64) error {
	return rm.nodeHost.RequestLeaderTransfer(groupID, targetNodeID)
}

// GetLeaderID returns the current leader node ID for the specified group
func (rm *RaftManager) GetLeaderID(groupID uint64) (uint64, bool, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if _, exists := rm.groups[groupID]; !exists {
		return 0, false, fmt.Errorf("raft group %d not found", groupID)
	}

	leaderID, valid, err := rm.nodeHost.GetLeaderID(groupID)
	if err != nil {
		return 0, false, err
	}

	return leaderID, valid, nil
}

// IsLeader checks if the current node is the leader of the specified group
func (rm *RaftManager) IsLeader(groupID uint64) bool {
	leaderID, valid, err := rm.GetLeaderID(groupID)
	if err != nil || !valid {
		return false
	}
	return leaderID == rm.config.NodeID
}

// GetNodeHost returns the Dragonboat NodeHost
func (rm *RaftManager) GetNodeHost() *dragonboat.NodeHost {
	return rm.nodeHost
}

// GetGroups returns all active Raft groups
func (rm *RaftManager) GetGroups() map[uint64]*RaftGroup {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	groups := make(map[uint64]*RaftGroup)
	for id, group := range rm.groups {
		groups[id] = group
	}
	return groups
}

// WaitForLeadershipReady waits for the raft group to have a leader within the timeout
func (rm *RaftManager) WaitForLeadershipReady(groupID uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if leaderID, exists, err := rm.GetLeaderID(groupID); err == nil && exists && leaderID != 0 {
			log.Printf("Raft group %d has leader: %d", groupID, leaderID)
			return nil
		}
		time.Sleep(100 * time.Millisecond) // Short sleep for responsiveness
	}

	return fmt.Errorf("timeout waiting for raft group %d to establish leadership", groupID)
}

// WatchLeadershipChanges provides real-time leadership change notifications
// This is more efficient than polling for applications that need immediate notifications
func (rm *RaftManager) WatchLeadershipChanges(groupID uint64, callback func(leaderID uint64, exists bool)) (func(), error) {
	rm.mu.RLock()
	if _, exists := rm.groups[groupID]; !exists {
		rm.mu.RUnlock()
		return nil, fmt.Errorf("raft group %d not found", groupID)
	}
	rm.mu.RUnlock()

	ctx, cancel := context.WithCancel(rm.ctx)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Leadership watcher for group %d recovered from panic: %v", groupID, r)
			}
		}()

		ticker := time.NewTicker(200 * time.Millisecond) // Even faster polling
		defer ticker.Stop()

		var lastLeader uint64
		var lastExists bool

		// Get initial state
		if leader, exists, err := rm.GetLeaderID(groupID); err == nil {
			lastLeader = leader
			lastExists = exists
			if callback != nil {
				callback(leader, exists)
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if leader, exists, err := rm.GetLeaderID(groupID); err == nil {
					if leader != lastLeader || exists != lastExists {
						log.Printf("Leadership change detected for group %d: leader %d->%d, exists %v->%v",
							groupID, lastLeader, leader, lastExists, exists)
						lastLeader = leader
						lastExists = exists
						if callback != nil {
							callback(leader, exists)
						}
					}
				} else {
					log.Printf("Error watching leadership for group %d: %v", groupID, err)
				}
			}
		}
	}()

	return cancel, nil
}

// Close gracefully shuts down the RaftManager
func (rm *RaftManager) Close() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	log.Printf("Shutting down RaftManager...")

	rm.cancel()

	for groupID := range rm.groups {
		if err := rm.nodeHost.StopCluster(groupID); err != nil {
			log.Printf("Error stopping raft group %d: %v", groupID, err)
		}
	}

	if rm.nodeHost != nil {
		rm.nodeHost.Stop()
	}

	log.Printf("RaftManager shutdown completed")
	return nil
}

// EnsureReadIndexConsistency ensures read consistency on followers
func (rm *RaftManager) EnsureReadIndexConsistency(ctx context.Context, groupID uint64) (uint64, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.nodeHost == nil {
		return 0, fmt.Errorf("NodeHost is not initialized")
	}

	if _, exists := rm.groups[groupID]; !exists {
		return 0, fmt.Errorf("raft group %d not found", groupID)
	}

	result, err := rm.nodeHost.ReadIndex(groupID, 5*time.Second)
	if err != nil {
		return 0, fmt.Errorf("ReadIndex operation failed for group %d: %v", groupID, err)
	}

	select {
	case <-result.AppliedC():
		log.Printf("ReadIndex completed for group %d", groupID)
		return 0, nil
	case <-ctx.Done():
		return 0, fmt.Errorf("ReadIndex operation timeout for group %d: %v", groupID, ctx.Err())
	}
}
