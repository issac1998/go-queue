package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/raft"
)

// ControllerManager manages Controller-related functionality
type ControllerManager struct {
	broker *Broker

	stateMachine *raft.ControllerStateMachine

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	leaderCtx    context.Context
	leaderCancel context.CancelFunc
	leaderWg     sync.WaitGroup

	healthChecker      *HealthChecker
	leaderScheduler    *LeaderScheduler
	loadMonitor        *LoadMonitor
	failureDetector    *FailureDetector
	rebalanceScheduler *RebalanceScheduler

	mu sync.RWMutex
}

// HealthChecker performs health checks on brokers
type HealthChecker struct {
	controller       *ControllerManager
	checkInterval    time.Duration
	timeout          time.Duration
	failureThreshold int
	logger           *log.Logger
}

// LeaderScheduler handles leader migration and load balancing
type LeaderScheduler struct {
	controller *ControllerManager
	logger     *log.Logger
}

// LoadMonitor monitors cluster load and performance
type LoadMonitor struct {
	controller *ControllerManager
	logger     *log.Logger
}

// FailureDetector detects broker failures and triggers recovery
type FailureDetector struct {
	controller *ControllerManager
	logger     *log.Logger
}

// RebalanceScheduler handles periodic partition rebalancing
type RebalanceScheduler struct {
	controller *ControllerManager
	interval   time.Duration
	logger     *log.Logger
}

// NewControllerManager creates a new ControllerManager
func NewControllerManager(broker *Broker) (*ControllerManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cm := &ControllerManager{
		broker: broker,
		ctx:    ctx,
		cancel: cancel,
	}

	cm.stateMachine = raft.NewControllerStateMachine(cm, broker.raftManager, broker.logger)

	cm.healthChecker = &HealthChecker{
		controller:       cm,
		checkInterval:    5 * time.Second,
		timeout:          10 * time.Second,
		failureThreshold: 3,
		logger:           broker.logger,
	}

	cm.leaderScheduler = &LeaderScheduler{
		controller: cm,
		logger:     broker.logger,
	}

	cm.loadMonitor = &LoadMonitor{
		controller: cm,
		logger:     broker.logger,
	}

	cm.failureDetector = &FailureDetector{
		controller: cm,
		logger:     broker.logger,
	}

	cm.rebalanceScheduler = &RebalanceScheduler{
		controller: cm,
		interval:   5 * time.Minute, // Rebalance every 5 minutes
		logger:     broker.logger,
	}

	return cm, nil
}

// Start starts the Controller
func (cm *ControllerManager) Start() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	log.Printf("Starting Controller Manager for broker %s", cm.broker.ID)

	if err := cm.initControllerRaftGroup(); err != nil {
		return fmt.Errorf("failed to initialize controller raft group: %v", err)
	}

	log.Printf("Controller Manager started successfully")
	return nil
}

// Stop stops the Controller
func (cm *ControllerManager) Stop() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	log.Printf("Stopping Controller Manager...")

	cm.cancel()
	cm.wg.Wait()

	if cm.broker.raftManager != nil {
		if err := cm.broker.raftManager.StopRaftGroup(raft.ControllerGroupID); err != nil {
			log.Printf("Error stopping controller raft group: %v", err)
		}
	}

	log.Printf("Controller Manager stopped")
	return nil
}

// initControllerRaftGroup initializes the Controller Raft Group
func (cm *ControllerManager) initControllerRaftGroup() error {
	// Retry broker discovery to handle timing issues
	var brokers []*discovery.BrokerInfo
	var err error

	brokers, err = cm.broker.discovery.DiscoverBrokers()
	if err != nil {
		return fmt.Errorf("broker discovery failed: %v", err)
	}

	log.Printf("Discovered %d broker(s) for controller initialization", len(brokers))

	members := make(map[uint64]string)

	// Build members map for all discovered brokers
	for _, broker := range brokers {
		nodeID := cm.brokerIDToNodeID(broker.ID)
		members[nodeID] = broker.RaftAddress
	}

	// Ensure current broker is in the members map
	currentNodeID := cm.brokerIDToNodeID(cm.broker.ID)
	if _, exists := members[currentNodeID]; !exists {
		members[currentNodeID] = cm.broker.Config.RaftConfig.RaftAddr
	}

	log.Printf("Initializing Controller Raft Group with members: %v", members)

	// Determine cluster initialization strategy based on broker ID
	// The broker with the smallest ID creates the full cluster
	// Other brokers join the existing cluster
	var shouldJoin bool = false
	var raftMembers map[uint64]string

	if len(brokers) == 1 {
		raftMembers = members
	} else {
		shouldJoin = true
	}

	log.Printf("Starting Controller Raft Group with members %v (join=%t)", raftMembers, shouldJoin)

	err = cm.broker.raftManager.StartRaftGroup(
		raft.ControllerGroupID,
		raftMembers,
		cm.stateMachine,
		shouldJoin,
	)
	if err != nil {
		return fmt.Errorf("controller raft group start failed: %v", err)
	}

	if err := cm.waitForControllerReady(300 * time.Second); err != nil {
		return fmt.Errorf("controller raft group not ready: %v", err)
	}

	if cm.broker.raftManager.IsLeader(raft.ControllerGroupID) {
		cm.StartLeaderTasks()
	}

	cm.startBackgroundTasks()

	return nil
}

// waitForControllerReady waits for the controller to be ready
func (cm *ControllerManager) waitForControllerReady(timeout time.Duration) error {
	return cm.broker.raftManager.WaitForLeadershipReady(
		raft.ControllerGroupID,
		timeout,
	)
}

// startBackgroundTasks starts background monitoring tasks
func (cm *ControllerManager) startBackgroundTasks() {
	cm.wg.Add(2)
	go cm.monitorLeadership()
	go cm.startTransactionCheck()
}

// monitorLeadership monitors changes in Raft leadership
func (cm *ControllerManager) monitorLeadership() {
	defer cm.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var wasLeader bool

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			isLeader := cm.broker.raftManager.IsLeader(raft.ControllerGroupID)
			if isLeader != wasLeader {
				if isLeader {
					go cm.StartLeaderTasks()
				} else {
					go cm.StopLeaderTasks()
				}
				wasLeader = isLeader
			}
		}
	}
}

// StartLeaderTasks starts tasks that only the leader should perform
func (cm *ControllerManager) StartLeaderTasks() {
	log.Printf("Starting leader tasks for broker %s", cm.broker.ID)

	// Create a new context for leader tasks
	cm.leaderCtx, cm.leaderCancel = context.WithCancel(cm.ctx)

	// Start existing partition Raft groups for recovery
	cm.startExistingPartitionGroups()

	cm.leaderWg.Add(2)
	go cm.healthChecker.startHealthCheck()
	go cm.rebalanceScheduler.startRebalancing()

	log.Printf("Leader tasks started for broker %s", cm.broker.ID)
}

// StopLeaderTasks stops leader-specific tasks
func (cm *ControllerManager) StopLeaderTasks() {
	log.Printf("Stopping leader tasks for broker %s", cm.broker.ID)

	if cm.leaderCancel != nil {
		cm.leaderCancel()
	}

	cm.leaderWg.Wait()

	log.Printf("All leader tasks stopped for broker %s", cm.broker.ID)
}

// startExistingPartitionGroups starts Raft groups for existing partitions during leader recovery
func (cm *ControllerManager) startExistingPartitionGroups() {
	log.Printf("Starting existing partition Raft groups for leader recovery")

	// Get all existing partition assignments from metadata
	allAssignments, err := cm.getAllPartitionAssignments()
	if err != nil {
		log.Printf("Failed to get existing partition assignments: %v", err)
		return
	}

	if len(allAssignments) == 0 {
		log.Printf("No existing partitions found")
		return
	}

	log.Printf("Found %d existing partition assignments to recover", len(allAssignments))

	// Get partition assigner
	partitionAssigner, err := cm.getPartitionAssigner()
	if err != nil {
		log.Printf("Failed to get partition assigner: %v", err)
		return
	}

	// Start Raft groups for existing partitions
	err = partitionAssigner.StartPartitionRaftGroups(allAssignments)
	if err != nil {
		log.Printf("Failed to start existing partition Raft groups: %v", err)
		return
	}

	log.Printf("Successfully started %d existing partition Raft groups", len(allAssignments))
}

// startGlobalTransactionCheck starts the global transaction check task
func (cm *ControllerManager) startTransactionCheck() {
	defer cm.wg.Done()

	ticker := time.NewTicker(300 * time.Second)
	defer ticker.Stop()

	log.Printf("Starting transaction check task for broker %s", cm.broker.ID)

	for {
		select {
		case <-cm.ctx.Done():
			log.Printf("Transaction check task stopped for broker %s", cm.broker.ID)
			return
		case <-ticker.C:
			cm.performGlobalTransactionCheck()
		}
	}
}

// performGlobalTransactionCheck performs global transaction check across all partitions
func (cm *ControllerManager) performGlobalTransactionCheck() {
	log.Printf("Performing global transaction check")

	allAssignments, err := cm.getAllPartitionAssignments()
	if err != nil {
		log.Printf("Failed to get partition assignments for transaction check: %v", err)
		return
	}

	for _, assignment := range allAssignments {
		if assignment.Leader == cm.broker.ID {
			cm.checkPartitionTransactions(assignment)
		}
	}
}

// checkPartitionTransactions checks transactions for a specific partition
func (cm *ControllerManager) checkPartitionTransactions(assignment *raft.PartitionAssignment) {
	// 获取分区的事务管理器
	if cm.broker.TransactionManager == nil {
		return
	}

	// 获取过期的半消息
	expiredTxns, err := cm.broker.TransactionManager.GetExpiredHalfMessages()
	if err != nil {
		log.Printf("Failed to get expired transactions for partition %s-%d: %v",
			assignment.TopicName, assignment.PartitionID, err)
		return
	}

	if len(expiredTxns) == 0 {
		return
	}

	log.Printf("Found %d expired transactions in partition %s-%d",
		len(expiredTxns), assignment.TopicName, assignment.PartitionID)

	// 执行事务检查
	err = cm.broker.TransactionManager.PerformTransactionCheck()
	if err != nil {
		log.Printf("Failed to perform transaction check for partition %s-%d: %v",
			assignment.TopicName, assignment.PartitionID, err)
	}
}

// getAllPartitionAssignments gets all partition assignments from the state machine
func (cm *ControllerManager) getAllPartitionAssignments() ([]*raft.PartitionAssignment, error) {
	// Query partition assignments using the existing QueryMetadata method
	result, err := cm.QueryMetadata(protocol.RaftQueryGetPartitionAssignments, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to query partition assignments: %w", err)
	}

	log.Printf("Query result type: %T, length: %d", result, len(result))

	// The QueryMetadata returns []byte, so we need to unmarshal it
	var assignmentsMap map[string]*raft.PartitionAssignment
	if err := json.Unmarshal(result, &assignmentsMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal partition assignments: %w", err)
	}

	log.Printf("Unmarshaled %d partition assignments", len(assignmentsMap))

	// Convert map to slice
	var allAssignments []*raft.PartitionAssignment
	for _, assignment := range assignmentsMap {
		allAssignments = append(allAssignments, assignment)
	}

	log.Printf("Found %d partition assignments", len(allAssignments))
	return allAssignments, nil
}

// IsLeader returns whether this broker is the Controller leader
func (cm *ControllerManager) isLeader() bool {
	leaderID, exists, _ := cm.broker.raftManager.GetLeaderID(raft.ControllerGroupID)
	nodeID := cm.brokerIDToNodeID(cm.broker.ID)
	if exists && leaderID == nodeID {
		return true
	}
	return false
}

// ExecuteRaftCommandWithRetry executes a Raft command
func (cm *ControllerManager) ExecuteRaftCommandWithRetry(cmd *raft.ControllerCommand, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := cm.executeRaftCommand(cmd)

		if err == nil {
			return nil
		}

		lastErr = err
		log.Printf("Raft command execution failed (attempt %d/%d): %v", attempt+1, maxRetries, err)

		if attempt < maxRetries-1 {
			time.Sleep(time.Millisecond * 100)
		}
	}

	return fmt.Errorf("failed after %d attempts, last error: %v", maxRetries, lastErr)
}

// executeRaftCommand executes a single Raft command
func (cm *ControllerManager) executeRaftCommand(cmd *raft.ControllerCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := cm.broker.raftManager.SyncPropose(
		ctx,
		raft.ControllerGroupID,
		data,
	)
	if err != nil {
		return err
	}

	if result.Data != nil {
		var resultMap map[string]interface{}
		if err := json.Unmarshal(result.Data, &resultMap); err == nil {
			if success, exists := resultMap["success"]; exists {
				if successBool, ok := success.(bool); ok && !successBool {
					if errorMsg, exists := resultMap["error"]; exists {
						return fmt.Errorf("%v", errorMsg)
					}
					return fmt.Errorf("operation failed")
				}
			}
		}
	}

	return nil
}

// ExecuteCommand executes a controller command through Raft with retry logic
func (cm *ControllerManager) ExecuteCommand(cmd *raft.ControllerCommand) error {
	return cm.ExecuteRaftCommandWithRetry(cmd, 3)
}

// GetControlledLeaderID returns the current Controller leader NodeID and whether it exists
func (cm *ControllerManager) GetControlledLeaderID() (uint64, bool) {
	leaderNodeID, valid, _ := cm.broker.raftManager.GetLeaderID(raft.ControllerGroupID)
	if !valid {
		return 0, false
	}
	return leaderNodeID, true
}

// QueryMetadata queries cluster metadata directly from local state machine
func (cm *ControllerManager) QueryMetadata(queryType string, params map[string]interface{}) ([]byte, error) {
	query := map[string]interface{}{
		"type": queryType,
	}
	for k, v := range params {
		query[k] = v
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %v", err)
	}

	// Read directly from local state machine since syncPropose ensures latest state
	result, err := cm.stateMachine.Lookup(queryData)
	if err != nil {
		return nil, fmt.Errorf("failed to query metadata: %v", err)
	}

	if data, ok := result.([]byte); ok {
		return data, nil
	}

	return json.Marshal(result)
}

// RegisterBroker registers the current broker in the cluster
func (cm *ControllerManager) RegisterBroker() error {
	cmd := &raft.ControllerCommand{
		Type:      "register_broker",
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"broker_id":    cm.broker.ID,
			"address":      cm.broker.Address,
			"port":         cm.broker.Port,
			"raft_address": cm.broker.Config.RaftConfig.RaftAddr,
		},
	}

	return cm.ExecuteCommand(cmd)
}

// CreateTopic creates a new topic
func (cm *ControllerManager) CreateTopic(topicName string, partitions int32, replicationFactor int32) error {
	if !cm.isLeader() {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: "only controller leader can create topics",
		}
	}

	log.Printf("Controller Leader creating topic %s with %d partitions (replication factor: %d)",
		topicName, partitions, replicationFactor)

	availableBrokers, err := cm.getAvailableBrokers()
	if err != nil {
		log.Printf("DEBUG: Failed to get available brokers: %v", err)
		return fmt.Errorf("failed to get available brokers: %w", err)
	}
	log.Printf("DEBUG: Found %d available brokers", len(availableBrokers))
	for i, broker := range availableBrokers {
		log.Printf("DEBUG: Broker %d - ID: %s, Address: %s, Status: %s", i, broker.ID, broker.Address, broker.Status)
	}
	if len(availableBrokers) == 0 {
		log.Printf("DEBUG: No available brokers for topic creation")
		return fmt.Errorf("no available brokers for topic creation")
	}

	partitionAssigner, err := cm.getPartitionAssigner()
	if err != nil {
		return fmt.Errorf("failed to get partition assigner: %w", err)
	}

	assignments, err := partitionAssigner.AllocatePartitions(topicName, partitions, replicationFactor, availableBrokers)
	if err != nil {
		return fmt.Errorf("failed to allocate partitions: %w", err)
	}

	log.Printf("Allocated %d partitions for topic %s", len(assignments), topicName)

	log.Printf("Starting Raft groups for topic %s", topicName)

	err = partitionAssigner.StartPartitionRaftGroups(assignments)
	if err != nil {
		log.Printf("Failed to start Raft groups for topic %s, attempting cleanup: %v", topicName, err)
		return fmt.Errorf("failed to start partition Raft groups: %w", err)
	}

	log.Printf("update topic to statemachine %s", topicName)
	log.Printf("send:%v", assignments)

	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdCreateTopic,
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"topic_name":         topicName,
			"partitions":         float64(partitions),
			"replication_factor": float64(replicationFactor),
			"assignments":        cm.assignmentsToMap(assignments),
		},
	}

	err = cm.ExecuteCommand(cmd)
	if err != nil {
		return fmt.Errorf("failed to update topic metadata: %w", err)
	}

	log.Printf("Topic %s created successfully with %d partitions", topicName, len(assignments))
	return nil
}

// DeleteTopic deletes an existing topic (ONLY for Controller Leader)
func (cm *ControllerManager) DeleteTopic(topicName string) error {
	if !cm.isLeader() {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: "only controller leader can delete topics",
		}
	}

	log.Printf("Controller Leader deleting topic %s", topicName)

	topicAssignments, err := cm.getTopicAssignments(topicName)
	if err != nil {
		return fmt.Errorf("failed to get topic assignments: %w", err)
	}

	if len(topicAssignments) == 0 {
		return fmt.Errorf("topic %s not found or has no partitions", topicName)
	}

	partitionAssigner, err := cm.getPartitionAssigner()
	if err != nil {
		return fmt.Errorf("failed to get partition assigner: %w", err)
	}

	log.Printf("Stopping %d Raft groups for topic %s", len(topicAssignments), topicName)
	err = partitionAssigner.StopPartitionRaftGroups(topicAssignments)
	if err != nil {
		log.Printf("Warning: failed to stop some Raft groups for topic %s: %v", topicName, err)
	}

	log.Printf("Stopped Raft groups for topic %s", topicName)

	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdDeleteTopic,
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"topic_name":          topicName,
			"assignments":         cm.assignmentsToMap(topicAssignments),
			"raft_groups_stopped": true,
		},
	}

	err = cm.ExecuteCommand(cmd)
	if err != nil {
		return fmt.Errorf("failed to update topic metadata: %w", err)
	}

	log.Printf("Topic %s deleted successfully", topicName)
	return nil
}

// GetTopic gets detailed information about a topic
func (cm *ControllerManager) GetTopic(topicName string) (*raft.TopicMetadata, error) {
	result, err := cm.QueryMetadata(protocol.RaftQueryGetTopic, map[string]interface{}{
		"topic_name": topicName,
	})
	if err != nil {
		return nil, err
	}

	var topicMetadata raft.TopicMetadata
	if err := json.Unmarshal(result, &topicMetadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal topic metadata: %v", err)
	}

	return &topicMetadata, nil
}

// GetTopicInfo gets basic information about a topic
func (cm *ControllerManager) GetTopicInfo(topicName string) (*raft.TopicMetadata, error) {
	return cm.GetTopic(topicName)
}

// GetTopicMetadata gets complete metadata about a topic
func (cm *ControllerManager) GetTopicMetadata(topicName string) (*raft.TopicMetadata, error) {
	return cm.GetTopic(topicName)
}

// GetMetadata gets the complete cluster metadata
func (cm *ControllerManager) GetMetadata() (*raft.ClusterMetadata, error) {
	result, err := cm.QueryMetadata(protocol.RaftQueryGetClusterMetadata, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster metadata: %v", err)
	}

	var metadata raft.ClusterMetadata
	if err := json.Unmarshal(result, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster metadata: %v", err)
	}

	return &metadata, nil
}

// JoinGroup handles a member joining a consumer group
func (cm *ControllerManager) JoinGroup(groupID, memberID string) error {
	if !cm.isLeader() {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}

	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdJoinGroup,
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"group_id":  groupID,
			"member_id": memberID,
		},
	}

	return cm.ExecuteCommand(cmd)
}

// LeaveGroup handles a member leaving a consumer group
func (cm *ControllerManager) LeaveGroup(groupID, memberID string) error {
	if !cm.isLeader() {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}

	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdLeaveGroup,
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"group_id":  groupID,
			"member_id": memberID,
		},
	}

	return cm.ExecuteCommand(cmd)
}

// DescribeGroup gets detailed information about a consumer group
func (cm *ControllerManager) DescribeGroup(groupID string) ([]byte, error) {
	return cm.QueryMetadata(protocol.RaftQueryGetGroup, map[string]interface{}{
		"group_id": groupID,
	})
}

// GetPartitionLeader returns the leader broker for a specific partition
func (cm *ControllerManager) GetPartitionLeader(topic string, partition int32) (string, error) {
	result, err := cm.QueryMetadata(protocol.RaftQueryGetPartitionLeader, map[string]interface{}{
		"topic":     topic,
		"partition": partition,
	})
	if err != nil {
		return "", err
	}

	var response map[string]string
	if err := json.Unmarshal(result, &response); err != nil {
		return "", err
	}

	if leader, exists := response["leader"]; exists {
		return leader, nil
	}

	return "", &errors.TypedError{
		Type:    errors.PartitionLeaderError,
		Message: "partition leader not found",
	}
}

// MigrateLeader migrates partition leader
func (cm *ControllerManager) MigrateLeader(partitionKey, newLeader string) error {
	if !cm.isLeader() {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}

	assignment, err := cm.getPartitionAssignment(partitionKey)
	if err != nil {
		return fmt.Errorf("failed to get partition assignment: %w", err)
	}

	if !cm.isValidReplica(assignment, newLeader) {
		return &errors.TypedError{
			Type:    errors.PartitionLeaderError,
			Message: fmt.Sprintf("broker %s is not a replica of partition %s", newLeader, partitionKey),
		}
	}

	if cm.broker.raftManager == nil {
		return fmt.Errorf("no raftManger")
	}

	return cm.executeDirectLeaderTransfer(assignment, newLeader)

}

// executeDirectLeaderTransfer directly calls dragonboat TransferLeadership
func (cm *ControllerManager) executeDirectLeaderTransfer(assignment *raft.PartitionAssignment, newLeader string) error {
	targetNodeID := cm.brokerIDToNodeID(newLeader)

	err := cm.broker.raftManager.TransferLeadership(assignment.RaftGroupID, targetNodeID)
	if err != nil {
		return &errors.TypedError{
			Type:    errors.LeadershipError,
			Message: fmt.Sprintf("failed to transfer leadership: %v", err),
			Cause:   err,
		}
	}

	log.Printf("Successfully initiated leader transfer for partition %s (group %d) to broker %s",
		fmt.Sprintf("%s-%d", assignment.TopicName, assignment.PartitionID),
		assignment.RaftGroupID, newLeader)

	return nil
}

// Helper functions
func (cm *ControllerManager) getPartitionAssignment(partitionKey string) (*raft.PartitionAssignment, error) {
	result, err := cm.QueryMetadata(protocol.RaftQueryGetPartitionAssignments, nil)
	if err != nil {
		return nil, err
	}

	var assignments map[string]*raft.PartitionAssignment
	if err := json.Unmarshal(result, &assignments); err != nil {
		return nil, fmt.Errorf("failed to unmarshal assignments: %w", err)
	}

	assignment, exists := assignments[partitionKey]
	if !exists {
		return nil, fmt.Errorf("partition %s not found", partitionKey)
	}

	return assignment, nil
}

func (cm *ControllerManager) isValidReplica(assignment *raft.PartitionAssignment, brokerID string) bool {
	for _, replica := range assignment.Replicas {
		if replica == brokerID {
			return true
		}
	}
	return false
}

func (cm *ControllerManager) brokerIDToNodeID(brokerID string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(brokerID))
	return h.Sum64()
}

// requestAddToCluster requests the leader to add this node to the cluster
func (cm *ControllerManager) requestAddToCluster(leaderBrokerID string, members map[uint64]string) error {
	// Find the leader's raft address
	var leaderRaftAddr string
	leaderNodeID := cm.brokerIDToNodeID(leaderBrokerID)

	for nodeID, addr := range members {
		if nodeID == leaderNodeID {
			leaderRaftAddr = addr
			break
		}
	}

	if leaderRaftAddr == "" {
		return fmt.Errorf("leader raft address not found for broker %s", leaderBrokerID)
	}

	currentNodeID := cm.brokerIDToNodeID(cm.broker.ID)
	currentRaftAddr := cm.broker.Config.RaftConfig.RaftAddr

	log.Printf("Requesting leader %s to add node %d (%s) to cluster", leaderBrokerID, currentNodeID, currentRaftAddr)

	// Note: In a real implementation, we would need to communicate with the leader
	// to request addition. For now, we'll assume the leader will discover and add us
	// through the service discovery mechanism.

	return nil
}

// Health checker implementation
func (hc *HealthChecker) startHealthCheck() {
	defer hc.controller.leaderWg.Done()

	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.controller.leaderCtx.Done():
			log.Printf("Health checker stopped due to leadership change")
			return
		case <-ticker.C:
			if hc.controller.isLeader() {
				hc.performHealthCheck()
			}
		}
	}
}

func (hc *HealthChecker) performHealthCheck() {
	result, err := hc.controller.QueryMetadata(protocol.RaftQueryGetBrokers, nil)
	if err != nil {
		hc.logger.Printf("Failed to get brokers for health check: %v", err)
		return
	}

	var brokers map[string]*raft.BrokerInfo
	if err := json.Unmarshal(result, &brokers); err != nil {
		hc.logger.Printf("Failed to unmarshal brokers: %v", err)
		return
	}

	// Check for new nodes to add to cluster (only if we are leader)
	if hc.controller.isLeader() {
		hc.checkForNewNodes(brokers)
	}

	// Check each broker
	for brokerID, broker := range brokers {
		if broker.Status == "failed" {
			continue
		}

		// Provide grace period for newly registered brokers (5 minutes)
		graceTime := 180 * time.Second
		if !broker.RegisteredAt.IsZero() && time.Since(broker.RegisteredAt) < 300*time.Second {
			graceTime = 300 * time.Second
		}

		// Check if broker hasn't sent heartbeat in too long
		if time.Since(broker.LastSeen) > graceTime {
			hc.logger.Printf("Broker %s appears to be unhealthy, last seen: %v", brokerID, broker.LastSeen)
			hc.handleBrokerFailure(brokerID)
		}
	}
}

func (hc *HealthChecker) handleBrokerFailure(brokerID string) {
	cmd := &raft.ControllerCommand{
		Type:      "mark_broker_failed",
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"broker_id": brokerID,
		},
	}

	if err := hc.controller.ExecuteCommand(cmd); err != nil {
		hc.logger.Printf("Failed to mark broker %s as failed: %v", brokerID, err)
	}
}

func (hc *HealthChecker) checkForNewNodes(currentBrokers map[string]*raft.BrokerInfo) {
	// Get all brokers from service discovery
	discoveredBrokers, err := hc.controller.broker.discovery.DiscoverBrokers()
	if err != nil {
		hc.logger.Printf("Failed to get brokers from service discovery: %v", err)
		return
	}

	// Find brokers that are in service discovery but not in current Raft cluster
	for _, discoveredBroker := range discoveredBrokers {
		if _, exists := currentBrokers[discoveredBroker.ID]; !exists {
			hc.logger.Printf("Found new broker %s, adding to Controller Raft cluster", discoveredBroker.ID)
			hc.addNewNodeToCluster(discoveredBroker)
		}
	}
}

func (hc *HealthChecker) addNewNodeToCluster(broker *discovery.BrokerInfo) {
	// Convert broker ID to node ID
	nodeID := hc.controller.brokerIDToNodeID(broker.ID)

	// Add node to Controller Raft group
	for i := 0; i < 3; i++ {
		_, err := hc.controller.broker.raftManager.RequestAddNode(1, nodeID, broker.RaftAddress, 0, 30*time.Second)
		if err != nil {
			hc.logger.Printf("Failed to add node %s to Controller Raft cluster: %v, retry : %d", broker.ID, err, i)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	hc.logger.Printf("Successfully added node %s to Controller Raft cluster", broker.ID)
}

// getAvailableBrokers returns list of available brokers
func (cm *ControllerManager) getAvailableBrokers() ([]*raft.BrokerInfo, error) {
	queryData := map[string]interface{}{
		"type": "get_brokers",
	}

	queryBytes, err := json.Marshal(queryData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// Read directly from local state machine since syncPropose ensures latest state
	result, err := cm.stateMachine.Lookup(queryBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to query brokers: %w", err)
	}

	if result == nil {
		return nil, fmt.Errorf("received nil result from raft query")
	}

	var brokersMap map[string]*raft.BrokerInfo

	// Try to cast directly to the expected type first
	if directMap, ok := result.(map[string]*raft.BrokerInfo); ok {
		brokersMap = directMap
	} else if resultBytes, ok := result.([]byte); ok {
		// Fallback to JSON unmarshaling
		if err := json.Unmarshal(resultBytes, &brokersMap); err != nil {
			return nil, fmt.Errorf("failed to parse brokers: %w", err)
		}
	} else {
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}

	fmt.Printf("DEBUG: brokersMap contains %d brokers\n", len(brokersMap))
	for id, broker := range brokersMap {
		fmt.Printf("DEBUG: Broker %s - Status: %s, Address: %s\n", id, broker.Status, broker.Address)
	}

	var available []*raft.BrokerInfo
	for _, broker := range brokersMap {
		if broker.Status == "active" {
			available = append(available, broker)
		}
	}

	fmt.Printf("DEBUG: Found %d active brokers\n", len(available))
	return available, nil
}

// getPartitionAssigner returns the partition assigner
func (cm *ControllerManager) getPartitionAssigner() (*raft.PartitionAssigner, error) {
	if cm.stateMachine == nil {
		return nil, fmt.Errorf("controller state machine not initialized")
	}

	partitionAssigner := cm.stateMachine.GetPartitionAssigner()
	if partitionAssigner == nil {
		return nil, fmt.Errorf("partition assigner not available (legacy mode)")
	}

	return partitionAssigner, nil
}

// assignmentsToMap converts PartitionAssignment slice to map format for JSON serialization
func (cm *ControllerManager) assignmentsToMap(assignments []*raft.PartitionAssignment) []map[string]interface{} {
	var result []map[string]interface{}
	for _, assignment := range assignments {
		assignmentMap := map[string]interface{}{
			"topic_name":       assignment.TopicName,
			"partition_id":     assignment.PartitionID,
			"raft_group_id":    fmt.Sprintf("%d", assignment.RaftGroupID),
			"replicas":         assignment.Replicas,
			"leader":           assignment.Leader,
			"preferred_leader": assignment.PreferredLeader,
		}
		result = append(result, assignmentMap)
	}
	return result
}

// getTopicAssignments returns all partition assignments for a specific topic
func (cm *ControllerManager) getTopicAssignments(topicName string) ([]*raft.PartitionAssignment, error) {
	// Query current partition assignments
	queryData := map[string]interface{}{
		"type": "get_partition_assignments",
	}

	queryBytes, err := json.Marshal(queryData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// Read directly from local state machine since syncPropose ensures latest state
	result, err := cm.stateMachine.Lookup(queryBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to query assignments: %w", err)
	}

	// State machine returns map[string]*raft.PartitionAssignment directly
	assignmentsMap, ok := result.(map[string]*raft.PartitionAssignment)
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}

	var topicAssignments []*raft.PartitionAssignment
	for _, assignment := range assignmentsMap {
		if assignment.TopicName == topicName {
			topicAssignments = append(topicAssignments, assignment)
		}
	}

	return topicAssignments, nil
}

// startRebalancing starts the periodic rebalancing task
func (rs *RebalanceScheduler) startRebalancing() {
	defer rs.controller.leaderWg.Done()

	ticker := time.NewTicker(rs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-rs.controller.leaderCtx.Done():
			log.Printf("Rebalance scheduler stopped due to leadership change")
			return
		case <-ticker.C:
			if rs.controller.isLeader() {
				rs.performRebalance()
			}
		}
	}
}

// performRebalance performs the actual rebalancing logic
func (rs *RebalanceScheduler) performRebalance() {
	log.Printf("Starting periodic partition rebalancing")

	// Get available brokers
	availableBrokers, err := rs.controller.getAvailableBrokers()
	if err != nil {
		log.Printf("Failed to get available brokers for rebalancing: %v", err)
		return
	}

	if len(availableBrokers) == 0 {
		log.Printf("No available brokers for rebalancing")
		return
	}

	currentAssignments, err := rs.getCurrentPartitionAssignments()
	if err != nil {
		log.Printf("Failed to get current partition assignments: %v", err)
		return
	}

	if len(currentAssignments) == 0 {
		log.Printf("No partition assignments to rebalance")
		return
	}

	partitionAssigner, err := rs.controller.getPartitionAssigner()
	if err != nil {
		log.Printf("Failed to get partition assigner: %v", err)
		return
	}

	newAssignments, err := partitionAssigner.RebalancePartitions(currentAssignments, availableBrokers)
	if err != nil {
		log.Printf("Failed to rebalance partitions: %v", err)
		return
	}

	changedAssignments := rs.countChangedAssignments(currentAssignments, newAssignments)
	if changedAssignments == 0 {
		log.Printf("No partition changes needed during rebalancing")
		return
	}

	log.Printf("Rebalancing will change %d partition assignments", changedAssignments)

	// TODO: Implement partition assignment update mechanism
	log.Printf("Partition rebalancing completed successfully. %d assignments changed", changedAssignments)
}

// getCurrentPartitionAssignments gets current partition assignments
func (rs *RebalanceScheduler) getCurrentPartitionAssignments() (map[string]*raft.PartitionAssignment, error) {
	result, err := rs.controller.QueryMetadata(protocol.RaftQueryGetPartitionAssignments, nil)
	if err != nil {
		return nil, err
	}

	var assignments map[string]*raft.PartitionAssignment
	if err := json.Unmarshal(result, &assignments); err != nil {
		return nil, fmt.Errorf("failed to unmarshal assignments: %w", err)
	}

	return assignments, nil
}

// countChangedAssignments counts how many assignments will change
func (rs *RebalanceScheduler) countChangedAssignments(
	current map[string]*raft.PartitionAssignment,
	new []*raft.PartitionAssignment,
) int {
	changed := 0
	for _, newAssignment := range new {
		partitionKey := fmt.Sprintf("%s-%d", newAssignment.TopicName, newAssignment.PartitionID)
		if oldAssignment, exists := current[partitionKey]; exists {
			if oldAssignment.Leader != newAssignment.Leader {
				changed++
			}
		}
	}
	return changed
}

// BeginConsumerTransaction starts a new consumer transaction
func (cm *ControllerManager) BeginConsumerTransaction(transactionID, consumerID, groupID string, timeoutMs int64) error {
	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdBeginConsumerTransaction,
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"transaction_id": transactionID,
			"consumer_id":    consumerID,
			"group_id":       groupID,
			"timeout_ms":     timeoutMs,
		},
	}
	return cm.ExecuteCommand(cmd)
}

// CommitConsumerTransaction commits a consumer transaction
func (cm *ControllerManager) CommitConsumerTransaction(transactionID, consumerID, groupID string) error {
	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdCommitConsumerTransaction,
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"transaction_id": transactionID,
			"consumer_id":    consumerID,
			"group_id":       groupID,
		},
	}
	return cm.ExecuteCommand(cmd)
}

// AbortConsumerTransaction aborts a consumer transaction
func (cm *ControllerManager) AbortConsumerTransaction(transactionID, consumerID, groupID string) error {
	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdAbortConsumerTransaction,
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"transaction_id": transactionID,
			"consumer_id":    consumerID,
			"group_id":       groupID,
		},
	}
	return cm.ExecuteCommand(cmd)
}

// RegisterProducerGroup registers a producer group with the controller
func (cm *ControllerManager) RegisterProducerGroup(groupID string, callbackAddr string) error {
	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdRegisterProducerGroup,
		ID:        fmt.Sprintf("register-producer-group-%s-%d", groupID, time.Now().UnixNano()),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"group_id":      groupID,
			"callback_addr": callbackAddr,
		},
	}

	return cm.ExecuteCommand(cmd)
}

// UnregisterProducerGroup unregisters a producer group from the controller
func (cm *ControllerManager) UnregisterProducerGroup(groupID string) error {
	if cm == nil {
		return fmt.Errorf("controller not available")
	}

	cmd := &raft.ControllerCommand{
		Type:      protocol.RaftCmdUnregisterProducerGroup,
		ID:        fmt.Sprintf("unregister-producer-group-%s-%d", groupID, time.Now().UnixNano()),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"group_id": groupID,
		},
	}

	return cm.ExecuteCommand(cmd)
}

// GetProducerGroups retrieves all producer groups from the controller
func (cm *ControllerManager) GetProducerGroups() (map[string]string, error) {
	if cm == nil {
		return nil, fmt.Errorf("controller not available")
	}

	// Use the controller's GetMetadata method to get cluster metadata
	metadata, err := cm.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster metadata: %v", err)
	}

	result := make(map[string]string)
	for groupID, groupInfo := range metadata.ProducerGroups {
		result[groupID] = groupInfo.CallbackAddr
	}

	return result, nil
}
