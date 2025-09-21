package raft

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	typederrors "github.com/issac1998/go-queue/internal/errors"
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
	logger   *log.Logger

	groups map[uint64]*RaftGroup
	mu     sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	// Unified leadership watching
	leadershipWatchers map[uint64][]func(uint64, bool) // groupID -> callbacks
	lastLeaderStates   map[uint64]leaderState          // groupID -> last known state
	watcherMutex       sync.RWMutex
	watcherCancel      context.CancelFunc
}

// RaftGroup represents an active Raft group
type RaftGroup struct {
	// Configuration
	GroupID      uint64
	Members      map[uint64]string // NodeID -> Address
	StateMachine statemachine.IStateMachine
	IsController bool
}

type leaderState struct {
	leaderID uint64
	exists   bool
}

// NewRaftManager creates a new RaftManager instance with real Dragonboat integration
func NewRaftManager(raftConfig *RaftConfig, dataDir string, logger *log.Logger) (*RaftManager, error) {
	if raftConfig == nil {
		raftConfig = &RaftConfig{
			RTTMillisecond:         200,
			HeartbeatRTT:           5,
			ElectionRTT:            10,
			CheckQuorum:            true,
			SnapshotEntries:        10000,
			CompactionOverhead:     5000,
			MaxInMemLogSize:        67108864,
			ControllerSnapshotFreq: 1000,
			NodeID:                 1,
			RaftAddr:               "localhost:63001",
			MutualTLS:              false,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	rm := &RaftManager{
		config:             raftConfig,
		dataDir:            dataDir,
		logger:             logger,
		groups:             make(map[uint64]*RaftGroup),
		ctx:                ctx,
		cancel:             cancel,
		leadershipWatchers: make(map[uint64][]func(uint64, bool)),
		lastLeaderStates:   make(map[uint64]leaderState),
	}

	// Start unified leadership watcher
	rm.startUnifiedLeadershipWatcher()

	// Initialize Dragonboat NodeHost
	if err := rm.initNodeHost(); err != nil {
		cancel()
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to initialize NodeHost", err)
	}

	return rm, nil
}

// initNodeHost initializes the Dragonboat NodeHost
func (rm *RaftManager) initNodeHost() error {
	nhConfig := config.NodeHostConfig{
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
		return &typederrors.TypedError{
			Type:    typederrors.LeadershipError,
			Message: "failed to create NodeHost",
			Cause:   err,
		}
	}

	rm.nodeHost = nodeHost
	rm.logger.Printf("Dragonboat NodeHost initialized successfully on %s", rm.config.RaftAddr)

	return nil
}

// StartRaftGroup starts a new Raft group with the specified configuration
// Following dragonboat specification:
// - For creating node (join=false): initialMembers contains all cluster members
// - For joining node (join=true): initialMembers contains existing cluster members for discovery
func (rm *RaftManager) StartRaftGroup(groupID uint64, members map[uint64]string, sm statemachine.IStateMachine, join bool) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.groups[groupID]; exists {
		return &typederrors.TypedError{
			Type:    typederrors.LeadershipError,
			Message: fmt.Sprintf("raft group %d already exists", groupID),
			Cause:   nil,
		}
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

	var initialMembers map[uint64]string
	if join {
		// For joining nodes, we still need to provide existing members for discovery
		// Remove current node from the members list since it's joining
		initialMembers = make(map[uint64]string)
		for nodeID, address := range members {
			if nodeID != rm.config.NodeID {
				initialMembers[nodeID] = address
			}
		}
	} else {
		initialMembers = members
	}

	rm.logger.Printf("Starting Raft group %d (join=%t) with initialMembers: %v", groupID, join, initialMembers)

	err := rm.nodeHost.StartCluster(initialMembers, join, func(uint64, uint64) statemachine.IStateMachine {
		return sm
	}, raftConfig)

	if err != nil {
		return &typederrors.TypedError{
			Type:    typederrors.LeadershipError,
			Message: fmt.Sprintf("failed to start raft group %d", groupID),
			Cause:   err,
		}
	}

	rm.groups[groupID] = &RaftGroup{
		GroupID:      groupID,
		Members:      members,
		StateMachine: sm,
		IsController: groupID == 1,
	}

	rm.logger.Printf("Raft group %d started successfully (join=%t)", groupID, join)
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
		return typederrors.NewTypedError(typederrors.GeneralError, fmt.Sprintf("failed to stop raft group %d", groupID), err)
	}

	delete(rm.groups, groupID)
	rm.logger.Printf("Raft group %d stopped successfully", groupID)
	return nil
}

// SyncPropose submits a proposal to the specified Raft group and waits for result
func (rm *RaftManager) SyncPropose(ctx context.Context, groupID uint64, data []byte) (statemachine.Result, error) {
	session := rm.nodeHost.GetNoOPSession(groupID)

	result, err := rm.nodeHost.SyncPropose(ctx, session, data)
	if err != nil {
		return statemachine.Result{}, typederrors.NewTypedError(typederrors.GeneralError, fmt.Sprintf("sync propose failed for group %d", groupID), err)
	}

	return result, nil
}

// TransferLeadership transfers leadership of the specified group to target node
func (rm *RaftManager) TransferLeadership(groupID uint64, targetNodeID uint64) error {
	return rm.nodeHost.RequestLeaderTransfer(groupID, targetNodeID)
}

// RequestAddNode adds a new node to an existing Raft cluster
func (rm *RaftManager) RequestAddNode(groupID uint64, nodeID uint64, address string, configChangeID uint64, timeout time.Duration) (*dragonboat.RequestState, error) {
	return rm.nodeHost.RequestAddNode(groupID, nodeID, address, configChangeID, timeout)
}

// RequestDeleteNode removes a node from an existing Raft cluster
func (rm *RaftManager) RequestDeleteNode(groupID uint64, nodeID uint64, configChangeID uint64, timeout time.Duration) (*dragonboat.RequestState, error) {
	return rm.nodeHost.RequestDeleteNode(groupID, nodeID, configChangeID, timeout)
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

// GetStateMachine returns the state machine for a specific Raft group
func (rm *RaftManager) GetStateMachine(groupID uint64) (statemachine.IStateMachine, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	group, exists := rm.groups[groupID]
	if !exists {
		return nil, &typederrors.TypedError{
			Type:    typederrors.LeadershipError,
			Message: fmt.Sprintf("raft group %d not found", groupID),
			Cause:   nil,
		}
	}

	return group.StateMachine, nil
}

// WaitForLeadershipReady waits for the raft group to have a leader within the timeout
func (rm *RaftManager) WaitForLeadershipReady(groupID uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	checkInterval := 5 * time.Second

	for time.Now().Before(deadline) {
		if leaderID, exists, err := rm.GetLeaderID(groupID); err == nil && exists && leaderID != 0 {
			rm.logger.Printf("find Raft group %d leader: %d", groupID, leaderID)
			return nil
		}
		rm.logger.Printf("not found , retry")
		// Sleep to avoid busy waiting and reduce CPU usage
		time.Sleep(checkInterval)
	}
	rm.logger.Printf("timeout waiting for raft group %d to establish leadership", groupID)
	return &typederrors.TypedError{
		Type:    typederrors.LeadershipError,
		Message: fmt.Sprintf("timeout waiting for raft group %d to establish leadership", groupID),
	}
}

// WatchLeadershipChanges monitors leadership changes for a specific group using unified watcher
// Returns a cancel function to stop watching
func (rm *RaftManager) WatchLeadershipChanges(groupID uint64, callback func(leaderID uint64, exists bool)) (func(), error) {
	rm.mu.RLock()
	if _, exists := rm.groups[groupID]; !exists {
		rm.mu.RUnlock()
		return nil, fmt.Errorf("raft group %d not found", groupID)
	}
	rm.mu.RUnlock()

	// Register callback with unified watcher
	rm.watcherMutex.Lock()
	rm.leadershipWatchers[groupID] = append(rm.leadershipWatchers[groupID], callback)
	callbackIndex := len(rm.leadershipWatchers[groupID]) - 1
	rm.watcherMutex.Unlock()

	// Trigger initial callback with current state
	go func() {
		currentLeader, exists, err := rm.GetLeaderID(groupID)
		if err == nil {
			callback(currentLeader, exists)
		}
	}()

	// Return cancel function that removes this specific callback
	return func() {
		rm.removeWatcher(groupID, callbackIndex)
	}, nil
}

// Close gracefully shuts down the RaftManager
func (rm *RaftManager) Close() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.logger.Printf("Shutting down RaftManager...")

	// Stop unified leadership watcher first
	if rm.watcherCancel != nil {
		rm.watcherCancel()
	}

	rm.cancel()

	for groupID := range rm.groups {
		if err := rm.nodeHost.StopCluster(groupID); err != nil {
			rm.logger.Printf("Error stopping raft group %d: %v", groupID, err)
		}
	}

	if rm.nodeHost != nil {
		rm.nodeHost.Stop()
	}

	rm.logger.Printf("RaftManager shutdown completed")
	return nil
}

// SyncRead performs a linearizable read operation on the specified Raft group
func (rm *RaftManager) SyncRead(ctx context.Context, groupID uint64, query interface{}) (interface{}, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.nodeHost == nil {
		return nil, fmt.Errorf("NodeHost is not initialized")
	}

	if _, exists := rm.groups[groupID]; !exists {
		return nil, fmt.Errorf("raft group %d not found", groupID)
	}

	result, err := rm.nodeHost.SyncRead(ctx, groupID, query)
	if err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, fmt.Sprintf("SyncRead operation failed for group %d", groupID), err)
	}

	return result, nil
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
		return 0, typederrors.NewTypedError(typederrors.GeneralError, fmt.Sprintf("ReadIndex operation failed for group %d", groupID), err)
	}

	select {
	case <-result.AppliedC():
		rm.logger.Printf("ReadIndex completed for group %d", groupID)
		return 0, nil
	case <-ctx.Done():
		return 0, fmt.Errorf("ReadIndex operation timeout for group %d: %v", groupID, ctx.Err())
	}
}

// GetGroupCount returns the number of active Raft groups
func (rm *RaftManager) GetGroupCount() int {
	return len(rm.groups)
}

// removeWatcher removes a specific callback from the leadership watchers
func (rm *RaftManager) removeWatcher(groupID uint64, callbackIndex int) {
	rm.watcherMutex.Lock()
	defer rm.watcherMutex.Unlock()

	if callbacks, exists := rm.leadershipWatchers[groupID]; exists && callbackIndex < len(callbacks) {
		// Set callback to nil instead of removing to maintain indices
		callbacks[callbackIndex] = nil

		// Clean up if all callbacks are nil
		allNil := true
		for _, cb := range callbacks {
			if cb != nil {
				allNil = false
				break
			}
		}

		if allNil {
			delete(rm.leadershipWatchers, groupID)
			delete(rm.lastLeaderStates, groupID)
		}
	}
}

// startUnifiedLeadershipWatcher starts a single goroutine to monitor all groups
func (rm *RaftManager) startUnifiedLeadershipWatcher() {
	watcherCtx, cancel := context.WithCancel(rm.ctx)
	rm.watcherCancel = cancel

	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-watcherCtx.Done():
				return
			case <-ticker.C:
				rm.checkAllGroupsLeadership()
			}
		}
	}()
}

// checkAllGroupsLeadership checks leadership changes for all registered groups
func (rm *RaftManager) checkAllGroupsLeadership() {
	rm.watcherMutex.RLock()
	groupsToCheck := make([]uint64, 0, len(rm.leadershipWatchers))
	for groupID := range rm.leadershipWatchers {
		groupsToCheck = append(groupsToCheck, groupID)
	}
	rm.watcherMutex.RUnlock()

	for _, groupID := range groupsToCheck {
		rm.checkGroupLeadership(groupID)
	}
}

// checkGroupLeadership checks leadership change for a specific group
func (rm *RaftManager) checkGroupLeadership(groupID uint64) {
	currentLeaderID, exists, err := rm.GetLeaderID(groupID)
	if err != nil {
		return
	}

	rm.watcherMutex.Lock()
	lastState, hasLastState := rm.lastLeaderStates[groupID]
	callbacks := rm.leadershipWatchers[groupID]
	rm.watcherMutex.Unlock()

	if !hasLastState || lastState.leaderID != currentLeaderID || lastState.exists != exists {
		rm.watcherMutex.Lock()
		rm.lastLeaderStates[groupID] = leaderState{
			leaderID: currentLeaderID,
			exists:   exists,
		}
		rm.watcherMutex.Unlock()

		for _, callback := range callbacks {
			if callback != nil {
				callback(currentLeaderID, exists)
			}
		}
	}
}
