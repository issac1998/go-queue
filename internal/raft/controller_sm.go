package raft

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/lni/dragonboat/v3/statemachine"
)

// ControllerStateMachine implements the Dragonboat state machine interface
// for managing cluster metadata
type ControllerStateMachine struct {
	manager           ControllerManager
	metadata          *ClusterMetadata
	partitionAssigner *PartitionAssigner
	mu                sync.RWMutex
}

// ClusterMetadata stores all cluster-wide metadata
type ClusterMetadata struct {
	ClusterID string                 `json:"cluster_id"`
	Brokers   map[string]*BrokerInfo `json:"brokers"`

	Topics               map[string]*TopicMetadata       `json:"topics"`
	PartitionAssignments map[string]*PartitionAssignment `json:"partition_assignments"`

	LeaderAssignments map[string]string `json:"leader_assignments"`

	ConsumerGroups map[string]*ConsumerGroupInfo `json:"consumer_groups"`

	Version    int64     `json:"version"`
	UpdateTime time.Time `json:"update_time"`
}

// BrokerInfo contains information about a broker
type BrokerInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Port    int    `json:"port"`
	Status  string `json:"status"`

	LoadMetrics *LoadMetrics `json:"load_metrics"`
	LastSeen    time.Time    `json:"last_seen"`

	RaftAddress string `json:"raft_address"`
}

// TopicMetadata contains metadata about a topic
type TopicMetadata struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int32             `json:"replication_factor"`
	CreatedAt         time.Time         `json:"created_at"`
	Config            map[string]string `json:"config"`
}

// PartitionAssignment defines how partitions are assigned to brokers
type PartitionAssignment struct {
	TopicName       string   `json:"topic_name"`
	PartitionID     int32    `json:"partition_id"`
	RaftGroupID     uint64   `json:"raft_group_id"`
	Replicas        []string `json:"replicas"`
	Leader          string   `json:"leader"`
	PreferredLeader string   `json:"preferred_leader"`
}

// LoadMetrics contains load information for a broker
type LoadMetrics struct {
	PartitionCount int32     `json:"partition_count"`
	LeaderCount    int32     `json:"leader_count"`
	MessageRate    float64   `json:"message_rate"`
	ByteRate       float64   `json:"byte_rate"`
	CPUUsage       float64   `json:"cpu_usage"`
	MemoryUsage    float64   `json:"memory_usage"`
	DiskUsage      float64   `json:"disk_usage"`
	LastUpdated    time.Time `json:"last_updated"`
}

// ConsumerGroupInfo contains information about a consumer group
type ConsumerGroupInfo struct {
	GroupID     string                      `json:"group_id"`
	Members     map[string]*GroupMemberInfo `json:"members"`
	State       string                      `json:"state"`
	LeaderID    string                      `json:"leader_id"`
	Protocol    string                      `json:"protocol"`
	CreatedAt   time.Time                   `json:"created_at"`
	LastUpdated time.Time                   `json:"last_updated"`
}

// GroupMemberInfo contains information about a group member
type GroupMemberInfo struct {
	MemberID   string    `json:"member_id"`
	ClientID   string    `json:"client_id"`
	ClientHost string    `json:"client_host"`
	JoinedAt   time.Time `json:"joined_at"`
	LastSeen   time.Time `json:"last_seen"`
}

// ControllerCommand represents a command to be executed by the controller
type ControllerCommand struct {
	Type      string                 `json:"type"`
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// ControllerManager interface for the controller manager
type ControllerManager interface {
	StartLeaderTasks()
	StopLeaderTasks()
}

// NewControllerStateMachine creates a controller state machine with Multi-Raft support
// TODO: KV?
func NewControllerStateMachine(manager ControllerManager, raftManager *RaftManager) *ControllerStateMachine {
	metadata := &ClusterMetadata{
		ClusterID:            fmt.Sprintf("cluster-%d", time.Now().Unix()),
		Brokers:              make(map[string]*BrokerInfo),
		Topics:               make(map[string]*TopicMetadata),
		PartitionAssignments: make(map[string]*PartitionAssignment),
		LeaderAssignments:    make(map[string]string),
		ConsumerGroups:       make(map[string]*ConsumerGroupInfo),
		Version:              1,
		UpdateTime:           time.Now(),
	}

	return &ControllerStateMachine{
		manager:           manager,
		metadata:          metadata,
		partitionAssigner: NewPartitionAssigner(metadata, raftManager),
	}
}

// Update invocked by dragonboat
func (csm *ControllerStateMachine) Update(data []byte) (statemachine.Result, error) {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	var cmd ControllerCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		log.Printf("Failed to unmarshal controller command: %v", err)
		return statemachine.Result{Value: 0}, err
	}

	result, err := csm.executeCommand(&cmd)
	if err != nil {
		log.Printf("Failed to execute controller command %s: %v", cmd.Type, err)
		return statemachine.Result{Value: 0}, err
	}

	csm.metadata.Version++
	csm.metadata.UpdateTime = time.Now()

	resultData, _ := json.Marshal(result)
	return statemachine.Result{
		Value: uint64(len(resultData)),
		Data:  resultData,
	}, nil
}

// executeCommand executes a controller command and returns the result
func (csm *ControllerStateMachine) executeCommand(cmd *ControllerCommand) (interface{}, error) {
	switch cmd.Type {
	case protocol.RaftCmdRegisterBroker:
		return csm.registerBroker(cmd.Data)
	case protocol.RaftCmdUnregisterBroker:
		return csm.unregisterBroker(cmd.Data)
	case protocol.RaftCmdCreateTopic:
		return csm.createTopic(cmd.Data)
	case protocol.RaftCmdDeleteTopic:
		return csm.deleteTopic(cmd.Data)
	case protocol.RaftCmdJoinGroup:
		return csm.joinGroup(cmd.Data)
	case protocol.RaftCmdLeaveGroup:
		return csm.leaveGroup(cmd.Data)
	case protocol.RaftCmdUpdateBrokerLoad:
		return csm.updateBrokerLoad(cmd.Data)
	case protocol.RaftCmdMarkBrokerFailed:
		return csm.markBrokerFailed(cmd.Data)
	case protocol.RaftCmdRebalancePartitions:
		return csm.rebalancePartitions(cmd.Data)
	default:
		return nil, fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// registerBroker registers a new broker in the cluster or updates existing one
func (csm *ControllerStateMachine) registerBroker(data map[string]interface{}) (interface{}, error) {
	brokerID := data["broker_id"].(string)
	address := data["address"].(string)
	port := int(data["port"].(float64))
	raftAddress := data["raft_address"].(string)

	// Check if broker already exists
	if existingBroker, exists := csm.metadata.Brokers[brokerID]; exists {
		// Update existing broker information
		existingBroker.Address = address
		existingBroker.Port = port
		existingBroker.RaftAddress = raftAddress
		existingBroker.Status = "active"
		existingBroker.LastSeen = time.Now()
		if existingBroker.LoadMetrics == nil {
			existingBroker.LoadMetrics = &LoadMetrics{
				LastUpdated: time.Now(),
			}
		}
		log.Printf("Updated existing broker %s at %s:%d", brokerID, address, port)
		return existingBroker, nil
	}

	// Create new broker
	broker := &BrokerInfo{
		ID:          brokerID,
		Address:     address,
		Port:        port,
		Status:      "active",
		RaftAddress: raftAddress,
		LoadMetrics: &LoadMetrics{
			LastUpdated: time.Now(),
		},
		LastSeen: time.Now(),
	}

	csm.metadata.Brokers[brokerID] = broker

	log.Printf("Registered new broker %s at %s:%d", brokerID, address, port)
	return broker, nil
}

func (csm *ControllerStateMachine) unregisterBroker(data map[string]interface{}) (interface{}, error) {
	brokerID := data["broker_id"].(string)
	delete(csm.metadata.Brokers, brokerID)
	log.Printf("Unregistered broker %s", brokerID)
	return map[string]string{"status": "success"}, nil
}

func (csm *ControllerStateMachine) createTopic(data map[string]interface{}) (interface{}, error) {
	// IMPORTANT: StateMachine should only update metadata, not perform allocation
	// The actual partition allocation should be done by Controller Leader BEFORE calling this
	topicName := data["topic_name"].(string)
	partitions := int32(data["partitions"].(float64))
	replicationFactor := int32(data["replication_factor"].(float64))

	// Check if topic already exists
	if _, exists := csm.metadata.Topics[topicName]; exists {
		return nil, fmt.Errorf("topic %s already exists", topicName)
	}

	// Parse pre-allocated assignments from the command data
	// These assignments should be calculated by Controller Leader before proposing
	assignmentsData, ok := data["assignments"]
	if !ok {
		return nil, fmt.Errorf("missing pre-allocated assignments in createTopic command")
	}

	// Convert assignments data to PartitionAssignment slice
	assignments, err := csm.parseAssignments(assignmentsData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse assignments: %w", err)
	}

	// Validate assignments
	for _, assignment := range assignments {
		if err := csm.validateAssignment(assignment); err != nil {
			return nil, fmt.Errorf("invalid assignment for partition %d: %w", assignment.PartitionID, err)
		}
	}

	// Create topic metadata
	topic := &TopicMetadata{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		CreatedAt:         time.Now(),
		Config:            make(map[string]string),
	}

	// Update metadata (this is the only thing StateMachine should do)
	csm.metadata.Topics[topicName] = topic
	for _, assignment := range assignments {
		partitionKey := fmt.Sprintf("%s-%d", topicName, assignment.PartitionID)
		csm.metadata.PartitionAssignments[partitionKey] = assignment
		csm.metadata.LeaderAssignments[partitionKey] = assignment.Leader
	}

	log.Printf("Topic %s created with %d partitions (metadata updated in StateMachine)", topicName, len(assignments))

	return map[string]interface{}{
		"topic":                    topic,
		"assignments":              assignments,
		"partition_groups_created": len(assignments),
		"status":                   "metadata_updated",
	}, nil
}

// parseAssignments converts assignment data from command to PartitionAssignment slice
func (csm *ControllerStateMachine) parseAssignments(assignmentsData interface{}) ([]*PartitionAssignment, error) {
	switch v := assignmentsData.(type) {
	case []*PartitionAssignment:
		return v, nil
	case []interface{}:
		var assignments []*PartitionAssignment
		for _, item := range v {
			if assignmentMap, ok := item.(map[string]interface{}); ok {
				assignment, err := csm.mapToPartitionAssignment(assignmentMap)
				if err != nil {
					return nil, err
				}
				assignments = append(assignments, assignment)
			} else {
				return nil, fmt.Errorf("invalid assignment format")
			}
		}
		return assignments, nil
	default:
		return nil, fmt.Errorf("unsupported assignments data type: %T", v)
	}
}

// mapToPartitionAssignment converts a map to PartitionAssignment
func (csm *ControllerStateMachine) mapToPartitionAssignment(data map[string]interface{}) (*PartitionAssignment, error) {
	assignment := &PartitionAssignment{}

	if v, ok := data["topic_name"]; ok {
		assignment.TopicName = v.(string)
	}

	if v, ok := data["partition_id"]; ok {
		assignment.PartitionID = int32(v.(float64))
	}

	if v, ok := data["raft_group_id"]; ok {
		assignment.RaftGroupID = uint64(v.(float64))
	}

	if v, ok := data["replicas"]; ok {
		if replicas, ok := v.([]interface{}); ok {
			for _, replica := range replicas {
				assignment.Replicas = append(assignment.Replicas, replica.(string))
			}
		}
	}

	if v, ok := data["leader"]; ok {
		assignment.Leader = v.(string)
	}

	if v, ok := data["preferred_leader"]; ok {
		assignment.PreferredLeader = v.(string)
	}

	return assignment, nil
}

func (csm *ControllerStateMachine) deleteTopic(data map[string]interface{}) (interface{}, error) {
	// IMPORTANT: StateMachine should only update metadata, not stop Raft groups
	// The actual Raft group stopping should be done by Controller Leader BEFORE calling this

	topicName := data["topic_name"].(string)

	// Check if topic exists
	_, exists := csm.metadata.Topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topicName)
	}

	log.Printf("Deleting topic %s metadata (Raft groups already stopped by Controller Leader)", topicName)

	// Parse assignments to be deleted from the command data
	assignmentsData, ok := data["assignments"]
	if !ok {
		// Fallback: find assignments by topic name
		log.Printf("No pre-computed assignments provided, finding by topic name")
		var partitionKeysToDelete []string
		for partitionKey, assignment := range csm.metadata.PartitionAssignments {
			if assignment.TopicName == topicName {
				partitionKeysToDelete = append(partitionKeysToDelete, partitionKey)
			}
		}

		// Remove partition assignments
		deletedPartitions := 0
		for _, partitionKey := range partitionKeysToDelete {
			delete(csm.metadata.PartitionAssignments, partitionKey)
			delete(csm.metadata.LeaderAssignments, partitionKey)
			deletedPartitions++
		}

		delete(csm.metadata.Topics, topicName)

		log.Printf("Topic %s metadata deleted: removed %d partitions", topicName, deletedPartitions)
		return map[string]interface{}{
			"status":             "metadata_deleted",
			"topic":              topicName,
			"deleted_partitions": deletedPartitions,
		}, nil
	}

	// Use pre-computed assignments from Controller Leader
	assignments, err := csm.parseAssignments(assignmentsData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse assignments: %w", err)
	}

	// Remove partition assignments (metadata update only)
	deletedPartitions := 0
	for _, assignment := range assignments {
		partitionKey := fmt.Sprintf("%s-%d", assignment.TopicName, assignment.PartitionID)
		delete(csm.metadata.PartitionAssignments, partitionKey)
		delete(csm.metadata.LeaderAssignments, partitionKey)
		deletedPartitions++
	}

	delete(csm.metadata.Topics, topicName)

	log.Printf("Topic %s metadata deleted: removed %d partitions (Raft groups already stopped)",
		topicName, deletedPartitions)

	return map[string]interface{}{
		"status":              "metadata_deleted",
		"topic":               topicName,
		"deleted_partitions":  deletedPartitions,
		"raft_groups_stopped": true, // Already stopped by Controller Leader
	}, nil
}

func (csm *ControllerStateMachine) joinGroup(data map[string]interface{}) (interface{}, error) {
	groupID := data["group_id"].(string)
	memberID := data["member_id"].(string)

	group, exists := csm.metadata.ConsumerGroups[groupID]
	if !exists {
		group = &ConsumerGroupInfo{
			GroupID:     groupID,
			Members:     make(map[string]*GroupMemberInfo),
			State:       "Empty",
			Protocol:    "range",
			CreatedAt:   time.Now(),
			LastUpdated: time.Now(),
		}
		csm.metadata.ConsumerGroups[groupID] = group
	}

	member := &GroupMemberInfo{
		MemberID:   memberID,
		ClientID:   memberID,
		ClientHost: "unknown",
		JoinedAt:   time.Now(),
		LastSeen:   time.Now(),
	}
	group.Members[memberID] = member

	// Update group state
	if len(group.Members) == 1 {
		group.State = "Stable"
		group.LeaderID = memberID
	}
	group.LastUpdated = time.Now()

	log.Printf("Member %s joined group %s", memberID, groupID)
	return map[string]string{"status": "success", "group_id": groupID, "member_id": memberID}, nil
}

func (csm *ControllerStateMachine) leaveGroup(data map[string]interface{}) (interface{}, error) {
	groupID := data["group_id"].(string)
	memberID := data["member_id"].(string)

	group, exists := csm.metadata.ConsumerGroups[groupID]
	if !exists {
		return nil, fmt.Errorf("group %s not found", groupID)
	}

	delete(group.Members, memberID)
	group.LastUpdated = time.Now()

	if len(group.Members) == 0 {
		group.State = "Empty"
		group.LeaderID = ""
	} else if group.LeaderID == memberID {
		for newLeaderID := range group.Members {
			group.LeaderID = newLeaderID
			break
		}
	}

	log.Printf("Member %s left group %s", memberID, groupID)
	return map[string]string{"status": "success", "group_id": groupID, "member_id": memberID}, nil
}

func (csm *ControllerStateMachine) migrateLeader(data map[string]interface{}) (interface{}, error) {
	partitionKey := data["partition_key"].(string)
	newLeader := data["new_leader"].(string)

	assignment, exists := csm.metadata.PartitionAssignments[partitionKey]
	if !exists {
		return nil, fmt.Errorf("partition %s not found", partitionKey)
	}

	// Validate that target broker is in replica list
	if !csm.isValidReplica(assignment, newLeader) {
		return nil, fmt.Errorf("broker %s is not a replica of partition %s", newLeader, partitionKey)
	}

	// In Multi-Raft architecture, always use Raft leader transfer
	return csm.executeRaftLeaderTransfer(assignment, partitionKey, newLeader)
}

// isValidReplica checks if the broker is in the replica list
func (csm *ControllerStateMachine) isValidReplica(assignment *PartitionAssignment, brokerID string) bool {
	for _, replica := range assignment.Replicas {
		if replica == brokerID {
			return true
		}
	}
	return false
}

// executeRaftLeaderTransfer performs leader transfer using dragonboat
func (csm *ControllerStateMachine) executeRaftLeaderTransfer(assignment *PartitionAssignment, partitionKey, newLeader string) (interface{}, error) {
	// Convert broker ID to node ID using the same logic as PartitionAssigner
	targetNodeID := csm.brokerIDToNodeID(newLeader)

	// Get the PartitionAssigner to perform the transfer
	multiRaftAssigner := csm.partitionAssigner

	// Transfer leadership using dragonboat through the assigner
	err := multiRaftAssigner.raftManager.TransferLeadership(assignment.RaftGroupID, targetNodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to transfer leadership: %w", err)
	}

	log.Printf("Initiated leader transfer for partition %s (group %d) to broker %s (node %d)",
		partitionKey, assignment.RaftGroupID, newLeader, targetNodeID)

	// The actual leader update will happen through the leader change watcher
	return map[string]interface{}{
		"status":      "success",
		"method":      "raft_transfer",
		"new_leader":  newLeader,
		"raft_group":  assignment.RaftGroupID,
		"target_node": targetNodeID,
	}, nil
}

// brokerIDToNodeID converts broker ID to node ID using consistent conversion
func (csm *ControllerStateMachine) brokerIDToNodeID(brokerID string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(brokerID))
	return h.Sum64()
}

func (csm *ControllerStateMachine) updatePartitionAssignment(data map[string]interface{}) (interface{}, error) {
	return map[string]string{"status": "success"}, nil
}

func (csm *ControllerStateMachine) updateBrokerLoad(data map[string]interface{}) (interface{}, error) {
	brokerID := data["broker_id"].(string)

	if broker, exists := csm.metadata.Brokers[brokerID]; exists {
		broker.LastSeen = time.Now()
		if broker.LoadMetrics != nil {
			broker.LoadMetrics.LastUpdated = time.Now()
		}
	}

	return map[string]string{"status": "success"}, nil
}

func (csm *ControllerStateMachine) markBrokerFailed(data map[string]interface{}) (interface{}, error) {
	brokerID := data["broker_id"].(string)

	if broker, exists := csm.metadata.Brokers[brokerID]; exists {
		broker.Status = "failed"
	}

	log.Printf("Marked broker %s as failed", brokerID)
	return map[string]string{"status": "success"}, nil
}

func (csm *ControllerStateMachine) rebalancePartitions(data map[string]interface{}) (interface{}, error) {
	log.Printf("Starting partition rebalancing")

	// Perform rebalancing
	availableBrokers := csm.getAvailableBrokers()
	newAssignments, err := csm.partitionAssigner.RebalancePartitions(csm.metadata.PartitionAssignments, availableBrokers)
	if err != nil {
		return nil, fmt.Errorf("failed to rebalance partitions: %w", err)
	}

	// Update metadata with new assignments
	changedAssignments := 0
	for _, assignment := range newAssignments {
		partitionKey := fmt.Sprintf("%s-%d", assignment.TopicName, assignment.PartitionID)

		if oldAssignment, exists := csm.metadata.PartitionAssignments[partitionKey]; exists {
			if oldAssignment.Leader != assignment.Leader {
				changedAssignments++
				log.Printf("Partition %s leader changed from %s to %s",
					partitionKey, oldAssignment.Leader, assignment.Leader)
			}
		}

		csm.metadata.PartitionAssignments[partitionKey] = assignment
		csm.metadata.LeaderAssignments[partitionKey] = assignment.Leader
	}

	log.Printf("Partition rebalancing completed. %d assignments changed", changedAssignments)

	return map[string]interface{}{
		"status":              "success",
		"total_partitions":    len(newAssignments),
		"changed_assignments": changedAssignments,
		"assignments":         newAssignments,
	}, nil
}

// Lookup implements the statemachine.IStateMachine interface
func (csm *ControllerStateMachine) Lookup(query interface{}) (interface{}, error) {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	// Convert query to []byte if needed
	var queryBytes []byte
	switch q := query.(type) {
	case []byte:
		queryBytes = q
	case string:
		queryBytes = []byte(q)
	default:
		return nil, fmt.Errorf("invalid query type: %T", query)
	}

	var queryObj map[string]interface{}
	if err := json.Unmarshal(queryBytes, &queryObj); err != nil {
		return nil, err
	}

	queryType := queryObj["type"].(string)

	switch queryType {
	case protocol.RaftQueryGetBrokers:
		return csm.metadata.Brokers, nil
	case protocol.RaftQueryGetTopics:
		return csm.metadata.Topics, nil
	case protocol.RaftQueryGetTopic:
		topicName := queryObj["topic_name"].(string)
		if topic, exists := csm.metadata.Topics[topicName]; exists {
			return topic, nil
		}
		return nil, fmt.Errorf("topic %s not found", topicName)
	case protocol.RaftQueryGetPartitionAssignments:
		return csm.metadata.PartitionAssignments, nil
	case protocol.RaftQueryGetClusterMetadata:
		return csm.metadata, nil
	case protocol.RaftQueryGetGroups:
		return csm.metadata.ConsumerGroups, nil
	case protocol.RaftQueryGetGroup:
		groupID := queryObj["group_id"].(string)
		if group, exists := csm.metadata.ConsumerGroups[groupID]; exists {
			return group, nil
		}
		return nil, fmt.Errorf("group %s not found", groupID)
	case protocol.RaftQueryGetPartitionLeader:
		topicName := queryObj["topic"].(string)
		partition := int32(queryObj["partition"].(float64))
		partitionKey := fmt.Sprintf("%s-%d", topicName, partition)
		if leader, exists := csm.metadata.LeaderAssignments[partitionKey]; exists {
			return map[string]string{"leader": leader}, nil
		}
		return map[string]string{"error": "partition not found"}, nil
	default:
		return nil, fmt.Errorf("unknown query type: %s", queryType)
	}
}

// SaveSnapshot implements the statemachine.IStateMachine interface
func (csm *ControllerStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	data, err := json.Marshal(csm.metadata)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// RecoverFromSnapshot implements the statemachine.IStateMachine interface
func (csm *ControllerStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	var metadata ClusterMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}

	csm.mu.Lock()
	csm.metadata = &metadata
	csm.mu.Unlock()

	log.Printf("Recovered controller state machine from snapshot, version: %d", metadata.Version)
	return nil
}

// Close implements the statemachine.IStateMachine interface
func (csm *ControllerStateMachine) Close() error {
	log.Printf("Closing controller state machine")
	return nil
}

// getAvailableBrokers returns list of active brokers
func (csm *ControllerStateMachine) getAvailableBrokers() []*BrokerInfo {
	var available []*BrokerInfo

	for _, broker := range csm.metadata.Brokers {
		if broker.Status == "active" {
			available = append(available, broker)
		}
	}

	return available
}

// validateAssignment validates a partition assignment
func (csm *ControllerStateMachine) validateAssignment(assignment *PartitionAssignment) error {
	if assignment == nil {
		return fmt.Errorf("assignment cannot be nil")
	}

	if assignment.TopicName == "" {
		return fmt.Errorf("topic name cannot be empty")
	}

	if assignment.PartitionID < 0 {
		return fmt.Errorf("partition ID cannot be negative")
	}

	if len(assignment.Replicas) == 0 {
		return fmt.Errorf("replicas cannot be empty")
	}

	if assignment.PreferredLeader == "" {
		return fmt.Errorf("preferred leader cannot be empty")
	}

	// Validate preferred leader is in replicas
	leaderFound := false
	for _, replica := range assignment.Replicas {
		if replica == assignment.PreferredLeader {
			leaderFound = true
			break
		}
	}

	if !leaderFound {
		return fmt.Errorf("preferred leader %s not found in replicas", assignment.PreferredLeader)
	}

	// Validate all brokers exist
	for _, replica := range assignment.Replicas {
		if _, exists := csm.metadata.Brokers[replica]; !exists {
			return fmt.Errorf("broker %s not found", replica)
		}
	}

	return nil
}

// GetHash implements the statemachine.IStateMachine interface
func (csm *ControllerStateMachine) GetHash() uint64 {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	return uint64(csm.metadata.Version)
}

// GetPartitionAssigner returns the partition assigner instance
func (csm *ControllerStateMachine) GetPartitionAssigner() *PartitionAssigner {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	return csm.partitionAssigner
}
