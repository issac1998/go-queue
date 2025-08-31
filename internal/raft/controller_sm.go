package raft

import (
	"encoding/json"
	"fmt"
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
	manager  ControllerManager
	metadata *ClusterMetadata
	mu       sync.RWMutex
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
	RaftPort    int    `json:"raft_port"`
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
	ISR             []string `json:"isr"`
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
	OnBecomeLeader()
	OnLoseLeadership()
}

// NewControllerStateMachine creates a new controller state machine
func NewControllerStateMachine(manager ControllerManager) *ControllerStateMachine {
	return &ControllerStateMachine{
		manager: manager,
		metadata: &ClusterMetadata{
			ClusterID:            fmt.Sprintf("cluster-%d", time.Now().Unix()),
			Brokers:              make(map[string]*BrokerInfo),
			Topics:               make(map[string]*TopicMetadata),
			PartitionAssignments: make(map[string]*PartitionAssignment),
			LeaderAssignments:    make(map[string]string),
			ConsumerGroups:       make(map[string]*ConsumerGroupInfo),
			Version:              1,
			UpdateTime:           time.Now(),
		},
	}
}

// Update implements the statemachine.IStateMachine interface - correct signature
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
	case protocol.RaftCmdMigrateLeader:
		return csm.migrateLeader(cmd.Data)
	case protocol.RaftCmdUpdatePartitionAssignment:
		return csm.updatePartitionAssignment(cmd.Data)
	case protocol.RaftCmdUpdateBrokerLoad:
		return csm.updateBrokerLoad(cmd.Data)
	case protocol.RaftCmdMarkBrokerFailed:
		return csm.markBrokerFailed(cmd.Data)
	default:
		return nil, fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// registerBroker registers a new broker in the cluster
func (csm *ControllerStateMachine) registerBroker(data map[string]interface{}) (interface{}, error) {
	brokerID := data["broker_id"].(string)
	address := data["address"].(string)
	port := int(data["port"].(float64))
	raftAddress := data["raft_address"].(string)
	raftPort := int(data["raft_port"].(float64))

	if _, exists := csm.metadata.Brokers[brokerID]; exists {
		return nil, fmt.Errorf("broker %s already exists", brokerID)
	}

	broker := &BrokerInfo{
		ID:          brokerID,
		Address:     address,
		Port:        port,
		Status:      "active",
		RaftAddress: raftAddress,
		RaftPort:    raftPort,
		LoadMetrics: &LoadMetrics{
			LastUpdated: time.Now(),
		},
		LastSeen: time.Now(),
	}

	csm.metadata.Brokers[brokerID] = broker

	log.Printf("Registered broker %s at %s:%d", brokerID, address, port)
	return broker, nil
}

func (csm *ControllerStateMachine) unregisterBroker(data map[string]interface{}) (interface{}, error) {
	brokerID := data["broker_id"].(string)
	delete(csm.metadata.Brokers, brokerID)
	log.Printf("Unregistered broker %s", brokerID)
	return map[string]string{"status": "success"}, nil
}

func (csm *ControllerStateMachine) createTopic(data map[string]interface{}) (interface{}, error) {
	topicName := data["topic_name"].(string)
	partitions := int32(data["partitions"].(float64))
	replicationFactor := int32(data["replication_factor"].(float64))

	topic := &TopicMetadata{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		CreatedAt:         time.Now(),
		Config:            make(map[string]string),
	}

	csm.metadata.Topics[topicName] = topic

	log.Printf("Created topic %s with %d partitions and replication factor %d", topicName, partitions, replicationFactor)
	return topic, nil
}

func (csm *ControllerStateMachine) deleteTopic(data map[string]interface{}) (interface{}, error) {
	topicName := data["topic_name"].(string)
	delete(csm.metadata.Topics, topicName)
	log.Printf("Deleted topic %s", topicName)
	return map[string]string{"status": "success"}, nil
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

	// Add or update the member
	member := &GroupMemberInfo{
		MemberID:   memberID,
		ClientID:   memberID,
		ClientHost: "unknown", // TODO: Get actual client host
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

	if assignment, exists := csm.metadata.PartitionAssignments[partitionKey]; exists {
		assignment.Leader = newLeader
		csm.metadata.LeaderAssignments[partitionKey] = newLeader
	}

	log.Printf("Migrated leader for partition %s to broker %s", partitionKey, newLeader)
	return map[string]string{"status": "success", "new_leader": newLeader}, nil
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

// GetHash implements the statemachine.IStateMachine interface
func (csm *ControllerStateMachine) GetHash() uint64 {
	csm.mu.RLock()
	defer csm.mu.RUnlock()

	return uint64(csm.metadata.Version)
}
