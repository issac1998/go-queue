package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/issac1998/go-queue/internal/raft"
)

// ControllerManager manages Controller-related functionality
type ControllerManager struct {
	broker *Broker

	// State machine reference
	stateMachine *raft.ControllerStateMachine

	// Background tasks
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Health check and monitoring
	healthChecker   *HealthChecker
	leaderScheduler *LeaderScheduler
	loadMonitor     *LoadMonitor
	failureDetector *FailureDetector

	// Synchronization
	mu sync.RWMutex
}

// HealthChecker performs health checks on brokers
type HealthChecker struct {
	controller       *ControllerManager
	checkInterval    time.Duration
	timeout          time.Duration
	failureThreshold int
}

// LeaderScheduler handles leader migration and load balancing
type LeaderScheduler struct {
	controller *ControllerManager
}

// LoadMonitor monitors cluster load and performance
type LoadMonitor struct {
	controller *ControllerManager
}

// FailureDetector detects broker failures and triggers recovery
type FailureDetector struct {
	controller *ControllerManager
}

// NewControllerManager creates a new ControllerManager
func NewControllerManager(broker *Broker) (*ControllerManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cm := &ControllerManager{
		broker: broker,
		ctx:    ctx,
		cancel: cancel,
	}

	// Create state machine
	cm.stateMachine = raft.NewControllerStateMachine(cm)

	cm.healthChecker = &HealthChecker{
		controller:       cm,
		checkInterval:    30 * time.Second,
		timeout:          10 * time.Second,
		failureThreshold: 3,
	}

	cm.leaderScheduler = &LeaderScheduler{
		controller: cm,
	}

	cm.loadMonitor = &LoadMonitor{
		controller: cm,
	}

	cm.failureDetector = &FailureDetector{
		controller: cm,
	}

	return cm, nil
}

// Start starts the Controller
func (cm *ControllerManager) Start() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	log.Printf("Starting Controller Manager for broker %s", cm.broker.ID)

	// Start Controller Raft Group
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

	// Stop background tasks
	cm.cancel()
	cm.wg.Wait()

	// Stop Controller Raft Group
	if cm.broker.raftManager != nil {
		if err := cm.broker.raftManager.StopRaftGroup(cm.broker.Config.RaftConfig.ControllerGroupID); err != nil {
			log.Printf("Error stopping controller raft group: %v", err)
		}
	}

	log.Printf("Controller Manager stopped")
	return nil
}

// initControllerRaftGroup initializes the Controller Raft Group
func (cm *ControllerManager) initControllerRaftGroup() error {
	// Discover other brokers
	brokers, err := cm.broker.discovery.DiscoverBrokers()
	if err != nil {
		return fmt.Errorf("broker discovery failed: %v", err)
	}

	// Prepare Controller Raft Group members
	members := make(map[uint64]string)
	for _, broker := range brokers {
		// Convert broker ID to uint64 for Raft
		nodeID := cm.brokerIDToNodeID(broker.ID)
		members[nodeID] = broker.RaftAddress
	}

	// Add current broker if not in the list
	currentNodeID := cm.broker.Config.RaftConfig.NodeID
	if _, exists := members[currentNodeID]; !exists {
		members[currentNodeID] = cm.broker.Config.RaftConfig.RaftAddr
	}

	log.Printf("Initializing Controller Raft Group with members: %v", members)

	// Determine if this is the first node (cluster bootstrap)
	isFirstNode := len(brokers) <= 1 // If only current broker or no other brokers discovered

	// Start Controller Raft Group
	err = cm.broker.raftManager.StartRaftGroup(
		cm.broker.Config.RaftConfig.ControllerGroupID,
		members,
		cm.stateMachine,
		!isFirstNode, // join = true if not first node
	)
	if err != nil {
		return fmt.Errorf("controller raft group start failed: %v", err)
	}

	// Wait for Raft Group to be ready
	if err := cm.waitForControllerReady(30 * time.Second); err != nil {
		return fmt.Errorf("controller raft group not ready: %v", err)
	}

	// Check if we are the leader and start leadership tasks if needed
	if cm.broker.raftManager.IsLeader(cm.broker.Config.RaftConfig.ControllerGroupID) {
		cm.OnBecomeLeader()
	}

	// Start background monitoring
	cm.startBackgroundTasks()

	return nil
}

// waitForControllerReady waits for the controller to be ready
func (cm *ControllerManager) waitForControllerReady(timeout time.Duration) error {
	return cm.broker.raftManager.WaitForLeadershipReady(
		cm.broker.Config.RaftConfig.ControllerGroupID,
		timeout,
	)
}

// startBackgroundTasks starts background monitoring tasks
func (cm *ControllerManager) startBackgroundTasks() {
	// Start leadership monitoring
	cm.wg.Add(1)
	go cm.monitorLeadership()
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
			isLeader := cm.broker.raftManager.IsLeader(cm.broker.Config.RaftConfig.ControllerGroupID)
			if isLeader != wasLeader {
				if isLeader {
					cm.OnBecomeLeader()
				} else {
					cm.OnLoseLeadership()
				}
				wasLeader = isLeader
			}
		}
	}
}

// OnBecomeLeader is called when this broker becomes the Controller leader
func (cm *ControllerManager) OnBecomeLeader() {
	log.Printf("Broker %s became Controller leader", cm.broker.ID)
	cm.startLeaderTasks()
}

// OnLoseLeadership is called when this broker loses Controller leadership
func (cm *ControllerManager) OnLoseLeadership() {
	log.Printf("Broker %s lost Controller leadership", cm.broker.ID)
	cm.stopLeaderTasks()
}

// startLeaderTasks starts tasks that only the leader should perform
func (cm *ControllerManager) startLeaderTasks() {
	// Start health checking
	cm.wg.Add(1)
	go cm.healthChecker.startHealthCheck()

	// Start load monitoring
	cm.wg.Add(1)
	go cm.loadMonitor.startMonitoring()

	// Start failure detection
	cm.wg.Add(1)
	go cm.failureDetector.startDetection()

	// Perform initial cluster health check
	cm.wg.Add(1)
	go cm.performFullHealthCheck()
}

// stopLeaderTasks stops leader-specific tasks
func (cm *ControllerManager) stopLeaderTasks() {
	// Tasks will stop when context is cancelled or leadership changes
	log.Printf("Stopping leader tasks for broker %s", cm.broker.ID)
}

// performFullHealthCheck performs a comprehensive health check
func (cm *ControllerManager) performFullHealthCheck() {
	defer cm.wg.Done()

	log.Printf("Performing full cluster health check...")

	// Register current broker
	if err := cm.RegisterBroker(); err != nil {
		log.Printf("Failed to register current broker: %v", err)
	}

	// Check for any failed brokers and trigger recovery
	// This would be implemented based on specific requirements
}

// IsLeader returns whether this broker is the Controller leader
func (cm *ControllerManager) isLeader() bool {
	leaderID, exists, _ := cm.broker.raftManager.GetLeaderID(cm.broker.Config.RaftConfig.ControllerGroupID)
	if exists && leaderID == cm.broker.Controller.brokerIDToNodeID(cm.broker.ID) {
		return true
	}
	return false
}

// ExecuteRaftCommandWithRetry executes a Raft command
func (cm *ControllerManager) ExecuteRaftCommandWithRetry(cmd *raft.ControllerCommand, maxRetries int) error {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// TODO: DO we really need to retry?
		err := cm.executeRaftCommand(cmd)
		if err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed after %d attempts, last error: %v", maxRetries, lastErr)
}

// executeRaftCommand executes a single Raft command
func (cm *ControllerManager) executeRaftCommand(cmd *raft.ControllerCommand) error {
	// Serialize command
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %v", err)
	}

	// Submit to Raft , this will fail if we're not the real leader
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = cm.broker.raftManager.SyncPropose(
		ctx,
		cm.broker.Config.RaftConfig.ControllerGroupID,
		data,
	)

	return err
}

// isNotLeaderError checks if the error indicates we're not the leader
func (cm *ControllerManager) isNotLeaderError(err error) bool {
	if err == nil {
		return false
	}
	// TODO: find out what is the err when it is not leader
	// Check for common "not leader" error patterns
	errStr := err.Error()
	return strings.Contains(errStr, "not leader") ||
		strings.Contains(errStr, "not the leader") ||
		strings.Contains(errStr, "leadership") ||
		strings.Contains(errStr, "invalid leader")
}

// ExecuteCommand executes a controller command through Raft with retry logic
// This is the main public interface for executing Raft commands
func (cm *ControllerManager) ExecuteCommand(cmd *raft.ControllerCommand) error {
	// Use retry logic to handle leader changes gracefully
	return cm.ExecuteRaftCommandWithRetry(cmd, 3) // Max 3 attempts
}

// GetControlledLeaderID returns the current Controller leader NodeID and whether it exists
func (cm *ControllerManager) GetControlledLeaderID() (uint64, bool) {
	leaderNodeID, valid, _ := cm.broker.raftManager.GetLeaderID(cm.broker.Config.RaftConfig.ControllerGroupID)
	if !valid {
		return 0, false
	}
	return leaderNodeID, true
}

// QueryMetadata queries cluster metadata
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// We use sync Read, or just read it from stateMachine?
	result, err := cm.broker.raftManager.SyncRead(
		ctx,
		cm.broker.Config.RaftConfig.ControllerGroupID,
		queryData,
	)
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
			"raft_port":    cm.broker.Config.RaftConfig.NodeID, // Simplified
		},
	}

	return cm.ExecuteCommand(cmd)
}

// CreateTopic creates a new topic
func (cm *ControllerManager) CreateTopic(topicName string, partitions int32, replicationFactor int32) error {
	if !cm.isLeader() {
		return fmt.Errorf("not controller leader")
	}

	cmd := &raft.ControllerCommand{
		Type:      "create_topic",
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"topic_name":         topicName,
			"partitions":         partitions,
			"replication_factor": replicationFactor,
		},
	}

	return cm.ExecuteCommand(cmd)
}

// GetPartitionLeader returns the leader broker for a specific partition
func (cm *ControllerManager) GetPartitionLeader(topic string, partition int32) (string, error) {
	result, err := cm.QueryMetadata("get_partition_leader", map[string]interface{}{
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

	return "", fmt.Errorf("partition leader not found")
}

// Helper methods

// brokerIDToNodeID converts a broker ID string to a uint64 node ID
func (cm *ControllerManager) brokerIDToNodeID(brokerID string) uint64 {
	// Simple hash function to convert string to uint64
	hash := uint64(0)
	for _, b := range []byte(brokerID) {
		hash = hash*31 + uint64(b)
	}
	return hash
}

// Health checker implementation
func (hc *HealthChecker) startHealthCheck() {
	defer hc.controller.wg.Done()

	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hc.controller.ctx.Done():
			return
		case <-ticker.C:
			// Use safer leader validation for critical background tasks
			if hc.controller.isLeader() {
				hc.performHealthCheck()

			}
		}
	}
}

func (hc *HealthChecker) performHealthCheck() {
	// Query cluster metadata to get broker list
	result, err := hc.controller.QueryMetadata("get_brokers", nil)
	if err != nil {
		log.Printf("Failed to get brokers for health check: %v", err)
		return
	}

	var brokers map[string]*raft.BrokerInfo
	if err := json.Unmarshal(result, &brokers); err != nil {
		log.Printf("Failed to unmarshal brokers: %v", err)
		return
	}

	// Check each broker
	for brokerID, broker := range brokers {
		if broker.Status == "failed" {
			continue
		}

		// Simple health check - in production this would be more sophisticated
		if time.Since(broker.LastSeen) > 60*time.Second {
			log.Printf("Broker %s appears to be unhealthy, last seen: %v", brokerID, broker.LastSeen)
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
		log.Printf("Failed to mark broker %s as failed: %v", brokerID, err)
	}
}

// Load monitor implementation
func (lm *LoadMonitor) startMonitoring() {
	defer lm.controller.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-lm.controller.ctx.Done():
			return
		case <-ticker.C:
			if lm.controller.isLeader() {
				lm.updateLoadMetrics()
			}
		}
	}
}

func (lm *LoadMonitor) updateLoadMetrics() {
	// Update load metrics for current broker
	cmd := &raft.ControllerCommand{
		Type:      "update_broker_load",
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"broker_id":       lm.controller.broker.ID,
			"partition_count": 0,   // TODO: Get actual count
			"leader_count":    0,   // TODO: Get actual count
			"message_rate":    0.0, // TODO: Get actual rate
			"cpu_usage":       0.0, // TODO: Get actual usage
		},
	}

	if err := lm.controller.ExecuteCommand(cmd); err != nil {
		log.Printf("Failed to update load metrics: %v", err)
	}
}

// Failure detector implementation
func (fd *FailureDetector) startDetection() {
	defer fd.controller.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fd.controller.ctx.Done():
			return
		case <-ticker.C:
			if fd.controller.isLeader() {
				fd.detectFailures()
			}
		}
	}
}

func (fd *FailureDetector) detectFailures() {
	// This would implement sophisticated failure detection logic
	// For now, it's a placeholder
	log.Printf("Running failure detection...")
}

// Leader scheduler implementation - placeholder for now
func (ls *LeaderScheduler) MigrateLeader(partitionKey, fromBroker, toBroker string) error {
	cmd := &raft.ControllerCommand{
		Type:      "migrate_leader",
		ID:        uuid.New().String(),
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"partition_key": partitionKey,
			"new_leader":    toBroker,
		},
	}

	return ls.controller.ExecuteCommand(cmd)
}
