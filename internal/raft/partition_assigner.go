package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math"
	"net"
	"sort"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplication"
	"github.com/issac1998/go-queue/internal/protocol"
)

// BrokerInterface defines the interface to access broker components
type BrokerInterface interface {
	GetCompressor() compression.Compressor
	GetDeduplicator() *deduplication.Deduplicator
}

// StartPartitionRaftGroupRequest represents the request to start a partition Raft group
type StartPartitionRaftGroupRequest struct {
	RaftGroupID uint64            `json:"raft_group_id"`
	TopicName   string            `json:"topic_name"`
	PartitionID int32             `json:"partition_id"`
	NodeMembers map[uint64]string `json:"node_members"`
	Join        bool              `json:"join"`
}

// StartPartitionRaftGroupResponse represents the response
type StartPartitionRaftGroupResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

// StopPartitionRaftGroupRequest represents the request to stop a partition Raft group
type StopPartitionRaftGroupRequest struct {
	RaftGroupID uint64 `json:"raft_group_id"`
	TopicName   string `json:"topic_name"`
	PartitionID int32  `json:"partition_id"`
}

// StopPartitionRaftGroupResponse represents the response
type StopPartitionRaftGroupResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

// PartitionAssigner implements partition assignment for Multi-Raft architecture
// Each partition is a separate Raft group with its own leader election
type PartitionAssigner struct {
	metadata    *ClusterMetadata
	raftManager *RaftManager
	broker      BrokerInterface
}

// NewPartitionAssigner creates a new Multi-Raft partition assigner
func NewPartitionAssigner(metadata *ClusterMetadata, raftManager *RaftManager) *PartitionAssigner {
	return &PartitionAssigner{
		metadata:    metadata,
		raftManager: raftManager,
		broker:      nil, //
	}
}

// SetBroker sets the broker interface for accessing components
func (pa *PartitionAssigner) SetBroker(broker BrokerInterface) {
	pa.broker = broker
}

// AllocatePartitions allocates partitions as independent Raft groups
func (pa *PartitionAssigner) AllocatePartitions(
	topicName string,
	partitions int32,
	replicationFactor int32,
	availableBrokers []*BrokerInfo,
) ([]*PartitionAssignment, error) {
	if len(availableBrokers) < int(replicationFactor) {
		return nil, fmt.Errorf("insufficient brokers: need %d, have %d",
			replicationFactor, len(availableBrokers))
	}

	assignments := make([]*PartitionAssignment, partitions)
	for i := int32(0); i < partitions; i++ {
		raftGroupID := pa.generateRaftGroupID(topicName, i)

		assignment := &PartitionAssignment{
			TopicName:   topicName,
			PartitionID: i,
			RaftGroupID: raftGroupID,
			Replicas:    make([]string, replicationFactor),
		}

		selectedBrokers := pa.selectLeastLoadedBrokers(availableBrokers, int(replicationFactor))
		for j, broker := range selectedBrokers {
			assignment.Replicas[j] = broker.ID
		}

		assignment.PreferredLeader = selectedBrokers[0].ID

		assignments[i] = assignment

		pa.updateBrokerLoadAfterAssignment(selectedBrokers, assignment.PreferredLeader)
	}

	return assignments, nil
}

// RebalancePartitions rebalances partitions considering Raft group dynamics
func (pa *PartitionAssigner) RebalancePartitions(
	currentAssignments map[string]*PartitionAssignment,
	availableBrokers []*BrokerInfo,
) ([]*PartitionAssignment, error) {

	if len(availableBrokers) == 0 {
		return nil, fmt.Errorf("no available brokers for rebalancing")
	}

	brokerLoads := pa.calculateBrokerLoads(currentAssignments, availableBrokers)
	avgLoad := pa.calculateAverageLoad(brokerLoads)
	threshold := avgLoad * 0.15 // 15% threshold for rebalancing

	var rebalancedAssignments []*PartitionAssignment
	var leaderTransfers []LeaderTransferPlan
	cnt := 0
	for _, assignment := range currentAssignments {
		newAssignment := *assignment

		// Get current actual leader from Raft group
		actualLeader, err := pa.getCurrentRaftLeader(assignment.RaftGroupID)
		if err == nil && actualLeader != "" {
			newAssignment.Leader = actualLeader
		}

		// Check if leader transfer is needed for load balancing
		if newAssignment.Leader != "" {
			leaderLoad := brokerLoads[newAssignment.Leader]
			if leaderLoad > avgLoad+threshold {
				bestReplica := pa.findBestLeaderReplica(&newAssignment, brokerLoads, avgLoad-threshold)
				if bestReplica != "" && bestReplica != newAssignment.Leader {
					leaderTransfers = append(leaderTransfers, LeaderTransferPlan{
						RaftGroupID: assignment.RaftGroupID,
						FromLeader:  newAssignment.Leader,
						ToLeader:    bestReplica,
					})
					newAssignment.Leader = bestReplica
					brokerLoads[assignment.Leader] -= 1.0
					brokerLoads[bestReplica] += 1.0
				}
			}
			cnt++
			// only do 3 transfer.
			if cnt == 3 {
				break
			}
		}

		rebalancedAssignments = append(rebalancedAssignments, &newAssignment)
	}

	// Execute leader transfers asynchronously
	if len(leaderTransfers) > 0 {
		go pa.executeLeaderTransfers(leaderTransfers)
	}

	return rebalancedAssignments, nil
}

// LeaderTransferPlan represents a planned leader transfer
type LeaderTransferPlan struct {
	RaftGroupID uint64
	FromLeader  string
	ToLeader    string
}

// executeLeaderTransfers executes planned leader transfers using dragonboat
func (pa *PartitionAssigner) executeLeaderTransfers(plans []LeaderTransferPlan) {
	log.Printf("Executing %d leader transfers for load balancing", len(plans))

	for _, plan := range plans {
		targetNodeID, err := pa.BrokerIDToNodeID(plan.ToLeader)
		if err != nil {
			log.Printf("Failed to convert broker ID %s to node ID: %v", plan.ToLeader, err)
			continue
		}

		err = pa.raftManager.TransferLeadership(plan.RaftGroupID, targetNodeID)
		if err != nil {
			log.Printf("Failed to transfer leadership for group %d from %s to %s: %v",
				plan.RaftGroupID, plan.FromLeader, plan.ToLeader, err)
		} else {
			log.Printf("Successfully initiated leader transfer for group %d from %s to %s",
				plan.RaftGroupID, plan.FromLeader, plan.ToLeader)
		}
	}
}

// getCurrentRaftLeader gets the current leader from the actual Raft group
func (pa *PartitionAssigner) getCurrentRaftLeader(raftGroupID uint64) (string, error) {
	leaderNodeID, valid, err := pa.raftManager.GetLeaderID(raftGroupID)
	if err != nil {
		return "", fmt.Errorf("failed to get leader for group %d: %w", raftGroupID, err)
	}

	if !valid || leaderNodeID == 0 {
		return "", fmt.Errorf("no valid leader for group %d", raftGroupID)
	}

	// Convert node ID back to broker ID
	brokerID, err := pa.nodeIDToBrokerID(leaderNodeID)
	if err != nil {
		return "", fmt.Errorf("failed to convert node ID %d to broker ID: %w", leaderNodeID, err)
	}

	return brokerID, nil
}

// findBestLeaderReplica finds the best replica to transfer leadership to
func (pa *PartitionAssigner) findBestLeaderReplica(
	assignment *PartitionAssignment,
	brokerLoads map[string]float64,
	targetLoad float64,
) string {
	bestReplica := ""
	bestLoad := math.MaxFloat64

	for _, replica := range assignment.Replicas {
		if replica != assignment.Leader {
			if load := brokerLoads[replica]; load < bestLoad && load <= targetLoad {
				bestLoad = load
				bestReplica = replica
			}
		}
	}

	return bestReplica
}

// StartPartitionRaftGroups starts Raft groups for newly assigned partitions
// This method coordinates the distributed startup across all involved brokers
func (pa *PartitionAssigner) StartPartitionRaftGroups(assignments []*PartitionAssignment) error {
	log.Printf("Starting Raft groups for %d partitions across multiple brokers", len(assignments))

	for _, assignment := range assignments {
		err := pa.startSinglePartitionRaftGroup(assignment)
		if err != nil {
			log.Printf("Failed to start Raft group for partition %s-%d: %v",
				assignment.TopicName, assignment.PartitionID, err)
			return err
		}
	}

	log.Printf("Successfully coordinated startup of %d partition Raft groups", len(assignments))

	// Wait for leadership establishment to ensure partitions are ready for use
	log.Printf("Waiting for leadership establishment across all partition Raft groups...")
	err := pa.waitForLeadershipEstablishment(assignments)
	if err != nil {
		log.Printf("Warning: some leadership establishments may have failed: %v", err)
	}

	log.Printf("Partition Raft groups startup and leadership establishment completed")
	return nil
}

// startSinglePartitionRaftGroup coordinates the startup of a single partition Raft group
func (pa *PartitionAssigner) startSinglePartitionRaftGroup(assignment *PartitionAssignment) error {
	log.Printf("Starting Raft group %d for partition %s-%d with replicas %v",
		assignment.RaftGroupID, assignment.TopicName, assignment.PartitionID, assignment.Replicas)

	// Convert broker IDs to node IDs and addresses
	nodeMembers := make(map[uint64]string)
	for _, brokerID := range assignment.Replicas {
		nodeID, err := pa.BrokerIDToNodeID(brokerID)
		if err != nil {
			return fmt.Errorf("failed to convert broker ID %s to node ID: %w", brokerID, err)
		}

		// Get broker info for address
		if broker, exists := pa.metadata.Brokers[brokerID]; exists {
			nodeMembers[nodeID] = broker.RaftAddress
		} else {
			return fmt.Errorf("broker %s not found in metadata", brokerID)
		}
	}

	for i, brokerID := range assignment.Replicas {
		err := pa.startRaftGroupOnBroker(assignment, nodeMembers, brokerID, i == 0)
		if err != nil {
			return fmt.Errorf("failed to start Raft group on broker %s: %w", brokerID, err)
		}
	}

	return nil
}

// startRaftGroupOnBroker starts the Raft group on a specific broker
func (pa *PartitionAssigner) startRaftGroupOnBroker(
	assignment *PartitionAssignment,
	nodeMembers map[uint64]string,
	brokerID string,
	isPreferredLeader bool,
) error {
	// Determine if this broker should create (join=false) or join (join=true) the cluster
	join := !isPreferredLeader // First broker (preferred leader) creates, others join

	log.Printf("Starting Raft group %d on broker %s (join=%t, isPreferredLeader=%t)",
		assignment.RaftGroupID, brokerID, join, isPreferredLeader)

	if brokerID == pa.getCurrentBrokerID() {
		// This is the current broker - start directly
		return pa.startRaftGroupLocally(assignment, nodeMembers, join)
	}
	return pa.sendStartRaftGroupCommand(assignment, nodeMembers, brokerID, join)

}

// startRaftGroupLocally starts the Raft group on the current broker
func (pa *PartitionAssigner) startRaftGroupLocally(
	assignment *PartitionAssignment,
	nodeMembers map[uint64]string,
	join bool,
) error {
	// Create partition state machine
	var compressor compression.Compressor
	var deduplicator *deduplication.Deduplicator

	if pa.broker != nil {
		compressor = pa.broker.GetCompressor()
		deduplicator = pa.broker.GetDeduplicator()
	} else {
		// Fallback to no compression/deduplication
		compressor, _ = compression.GetCompressor(compression.None)
		deduplicator = deduplication.NewDeduplicator(&deduplication.Config{Enabled: false})
	}

	stateMachine, err := NewPartitionStateMachine(assignment.TopicName, assignment.PartitionID, pa.raftManager.dataDir, compressor, deduplicator)
	if err != nil {
		return fmt.Errorf("failed to create partition state machine: %w", err)
	}

	// Start the Raft group with correct join parameter
	err = pa.raftManager.StartRaftGroup(
		assignment.RaftGroupID,
		nodeMembers,
		stateMachine,
		join,
	)
	if err != nil {
		return fmt.Errorf("failed to start local Raft group %d: %w", assignment.RaftGroupID, err)
	}

	log.Printf("Successfully started Raft group %d locally (join=%t)", assignment.RaftGroupID, join)
	return nil
}

// sendStartRaftGroupCommand sends a command to a remote broker to start the Raft group
func (pa *PartitionAssigner) sendStartRaftGroupCommand(
	assignment *PartitionAssignment,
	nodeMembers map[uint64]string,
	targetBrokerID string,
	join bool,
) error {
	// Get target broker info
	broker, exists := pa.metadata.Brokers[targetBrokerID]
	if !exists {
		return fmt.Errorf("target broker %s not found", targetBrokerID)
	}

	log.Printf("Sending start Raft group command to broker %s at %s (join=%t)",
		targetBrokerID, broker.Address, join)

	request := &StartPartitionRaftGroupRequest{
		RaftGroupID: assignment.RaftGroupID,
		TopicName:   assignment.TopicName,
		PartitionID: assignment.PartitionID,
		NodeMembers: nodeMembers,
		Join:        join,
	}

	resp, err := pa.sendStartPartitionRaftGroupRequest(broker.Address, broker.Port, request)
	if err != nil {
		return fmt.Errorf("failed to send request to broker %s:%d: %w",
			broker.Address, broker.Port, err)
	}
	if resp.Success {
		log.Printf("Successfully sent StartPartitionRaftGroup request to broker %s:%d",
			broker.Address, broker.Port)
	} else {
		return fmt.Errorf("failed to send request to broker %s:%d: %s",
			broker.Address, broker.Port, resp.Error)
	}
	return nil
}

func (pa *PartitionAssigner) sendStopPartitionRaftGroupRequest(
	brokerAddress string,
	brokerPort int,
	request *StopPartitionRaftGroupRequest,
) (*StopPartitionRaftGroupResponse, error) {
	addr := fmt.Sprintf("%s:%d", brokerAddress, brokerPort)
	conn, err := protocol.ConnectToSpecificBroker(addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	log.Printf("Connected to broker at %s for StopPartitionRaftGroup request", addr)

	if err := binary.Write(conn, binary.BigEndian, protocol.StopPartitionRaftGroupRequestType); err != nil {
		return nil, fmt.Errorf("failed to write request type: %w", err)
	}

	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize request: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	// Send request data
	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to write request data: %w", err)
	}

	log.Printf("Sent StopPartitionRaftGroup request: GroupID=%d", request.RaftGroupID)

	// Receive response
	return receiveStopPartitionRaftGroupResponse(conn)
}

// receiveStopPartitionRaftGroupResponse receives the response from the remote broker
func receiveStopPartitionRaftGroupResponse(conn net.Conn) (*StopPartitionRaftGroupResponse, error) {
	// Read response data length
	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	// Read response data
	responseData := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	// Parse response
	var response StopPartitionRaftGroupResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	log.Printf("Received StopPartitionRaftGroup response: Success=%t, Message=%s",
		response.Success, response.Message)

	if !response.Success {
		return &response, fmt.Errorf("remote broker returned error: %s", response.Error)
	}

	return &response, nil
}

// getCurrentBrokerID returns the current broker's ID
func (pa *PartitionAssigner) getCurrentBrokerID() string {
	// This would be injected or available from the broker context
	// For now, we'll implement this as a method that needs to be set
	return fmt.Sprintf("%d", pa.raftManager.config.NodeID) // Convert uint64 to string
}

// waitForLeadershipEstablishment waits for all partition Raft groups to elect leaders
func (pa *PartitionAssigner) waitForLeadershipEstablishment(assignments []*PartitionAssignment) error {
	// 30s is enough?
	timeout := 30 * time.Second
	// todo: do we need to concurrently check ?
	for _, assignment := range assignments {
		err := pa.raftManager.WaitForLeadershipReady(assignment.RaftGroupID, timeout)
		if err != nil {
			log.Printf("Warning: leadership not established for group %d within timeout: %v",
				assignment.RaftGroupID, err)
			continue
		}

		actualLeader, err := pa.getCurrentRaftLeader(assignment.RaftGroupID)
		if err != nil {
			log.Printf("Warning: failed to get current leader for group %d: %v",
				assignment.RaftGroupID, err)
			continue
		}

		assignment.Leader = actualLeader
		partitionKey := fmt.Sprintf("%s-%d", assignment.TopicName, assignment.PartitionID)
		pa.metadata.LeaderAssignments[partitionKey] = actualLeader

		if assignment.PreferredLeader != "" && actualLeader != assignment.PreferredLeader {
			log.Printf("Transferring leadership for partition %s from %s to preferred leader %s",
				partitionKey, actualLeader, assignment.PreferredLeader)

			preferredNodeID, err := pa.BrokerIDToNodeID(assignment.PreferredLeader)
			if err != nil {
				log.Printf("Warning: failed to convert preferred leader %s to node ID: %v",
					assignment.PreferredLeader, err)
				continue
			}

			err = pa.raftManager.TransferLeadership(assignment.RaftGroupID, preferredNodeID)
			if err != nil {
				log.Printf("Warning: failed to transfer leadership to preferred leader %s: %v",
					assignment.PreferredLeader, err)
				continue
			}

			time.Sleep(2 * time.Second)
			newLeader, err := pa.getCurrentRaftLeader(assignment.RaftGroupID)
			if err == nil && newLeader == assignment.PreferredLeader {
				assignment.Leader = newLeader
				pa.metadata.LeaderAssignments[partitionKey] = newLeader
				log.Printf("Successfully transferred leadership for partition %s to %s",
					partitionKey, newLeader)
			} else {
				log.Printf("Warning: leadership transfer may not have completed for partition %s",
					partitionKey)
			}
		}
	}

	return nil
}

func (pa *PartitionAssigner) nodeIDToBrokerID(nodeID uint64) (string, error) {
	// Reverse lookup in broker metadata
	for brokerID := range pa.metadata.Brokers {
		if expectedNodeID, err := pa.BrokerIDToNodeID(brokerID); err == nil && expectedNodeID == nodeID {
			return brokerID, nil
		}
	}
	return "", fmt.Errorf("no broker found for node ID %d", nodeID)
}

// NodeIDToBrokerID converts a node ID back to broker ID (public method)
func (pa *PartitionAssigner) NodeIDToBrokerID(nodeID uint64) (string, error) {
	return pa.nodeIDToBrokerID(nodeID)
}

// BrokerIDToNodeID converts broker ID to node ID (public method)
func (pa *PartitionAssigner) BrokerIDToNodeID(brokerID string) (uint64, error) {
	h := fnv.New64a()
	h.Write([]byte(brokerID))
	return h.Sum64(), nil
}

// Reuse helper functions from the original assigner
func (pa *PartitionAssigner) generateRaftGroupID(topicName string, partitionID int32) uint64 {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%s-%d", topicName, partitionID)))
	return h.Sum64()
}

func (pa *PartitionAssigner) calculateBrokerLoads(
	assignments map[string]*PartitionAssignment,
	brokers []*BrokerInfo,
) map[string]float64 {
	loads := make(map[string]float64)

	for _, broker := range brokers {
		if broker.LoadMetrics != nil {
			cpuWeight := 0.3
			memoryWeight := 0.2
			partitionWeight := 0.3
			leaderWeight := 0.2

			load := cpuWeight*broker.LoadMetrics.CPUUsage +
				memoryWeight*broker.LoadMetrics.MemoryUsage +
				partitionWeight*float64(broker.LoadMetrics.PartitionCount)/10.0 +
				leaderWeight*float64(broker.LoadMetrics.LeaderCount)/5.0

			loads[broker.ID] = load
		} else {
			loads[broker.ID] = 0.0
		}
	}

	// Add partition-based load
	for _, assignment := range assignments {
		if assignment.Leader != "" {
			loads[assignment.Leader] += 1.0 // Leader load
		}
		for _, replica := range assignment.Replicas {
			if replica != assignment.Leader {
				loads[replica] += 0.5 // Follower load
			}
		}
	}

	return loads
}

func (pa *PartitionAssigner) calculateAverageLoad(loads map[string]float64) float64 {
	if len(loads) == 0 {
		return 0.0
	}

	total := 0.0
	for _, load := range loads {
		total += load
	}

	return total / float64(len(loads))
}

// selectLeastLoadedBrokers selects the least loaded brokers for replica placement
func (pa *PartitionAssigner) selectLeastLoadedBrokers(availableBrokers []*BrokerInfo, count int) []*BrokerInfo {
	if count > len(availableBrokers) {
		count = len(availableBrokers)
	}

	brokers := make([]*BrokerInfo, len(availableBrokers))
	copy(brokers, availableBrokers)

	sort.Slice(brokers, func(i, j int) bool {
		return pa.getBrokerLoad(brokers[i]) < pa.getBrokerLoad(brokers[j])
	})

	return brokers[:count]
}

// getBrokerLoad calculates the current load of a broker
func (pa *PartitionAssigner) getBrokerLoad(broker *BrokerInfo) float64 {
	if broker.LoadMetrics == nil {
		return 0.0
	}

	cpuWeight := 0.3
	memoryWeight := 0.25
	partitionWeight := 0.25
	leaderWeight := 0.2

	load := cpuWeight*broker.LoadMetrics.CPUUsage +
		memoryWeight*broker.LoadMetrics.MemoryUsage +
		partitionWeight*float64(broker.LoadMetrics.PartitionCount)/10.0 +
		leaderWeight*float64(broker.LoadMetrics.LeaderCount)/5.0

	return load
}

// updateBrokerLoadAfterAssignment updates broker load metrics after assignment for next iteration
func (pa *PartitionAssigner) updateBrokerLoadAfterAssignment(selectedBrokers []*BrokerInfo, leaderID string) {
	for i, broker := range selectedBrokers {
		if broker.LoadMetrics == nil {
			broker.LoadMetrics = &LoadMetrics{}
		}

		broker.LoadMetrics.PartitionCount++

		if i == 0 && broker.ID == leaderID {
			broker.LoadMetrics.LeaderCount++
		}

		broker.LoadMetrics.LastUpdated = time.Now()
	}
}

func (pa *PartitionAssigner) sendStartPartitionRaftGroupRequest(
	brokerAddress string,
	brokerPort int,
	request *StartPartitionRaftGroupRequest,
) (*StartPartitionRaftGroupResponse, error) {
	addr := fmt.Sprintf("%s:%d", brokerAddress, brokerPort)
	conn, err := protocol.ConnectToSpecificBroker(addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))

	log.Printf("Connected to broker at %s for StartPartitionRaftGroup request", addr)

	if err := binary.Write(conn, binary.BigEndian, protocol.StartPartitionRaftGroupRequestType); err != nil {
		return nil, fmt.Errorf("failed to write request type: %w", err)
	}

	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize request: %w", err)
	}

	if err := binary.Write(conn, binary.BigEndian, int32(len(requestData))); err != nil {
		return nil, fmt.Errorf("failed to write request length: %w", err)
	}

	if _, err := conn.Write(requestData); err != nil {
		return nil, fmt.Errorf("failed to write request data: %w", err)
	}

	log.Printf("Sent StartPartitionRaftGroup request: GroupID=%d, Join=%t",
		request.RaftGroupID, request.Join)

	return receiveStartPartitionRaftGroupResponse(conn)
}

// receiveStartPartitionRaftGroupResponse receives the response from the remote broker
func receiveStartPartitionRaftGroupResponse(conn net.Conn) (*StartPartitionRaftGroupResponse, error) {
	var dataLength int32
	if err := binary.Read(conn, binary.BigEndian, &dataLength); err != nil {
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	responseData := make([]byte, dataLength)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	var response StartPartitionRaftGroupResponse
	if err := json.Unmarshal(responseData, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	log.Printf("Received StartPartitionRaftGroup response: Success=%t, Message=%s",
		response.Success, response.Message)

	if !response.Success {
		return &response, fmt.Errorf("remote broker returned error: %s", response.Error)
	}

	return &response, nil
}

// StopPartitionRaftGroups stops and removes Raft groups for deleted partitions
// This method coordinates the distributed shutdown across all involved brokers
func (pa *PartitionAssigner) StopPartitionRaftGroups(assignments []*PartitionAssignment) error {
	log.Printf("Stopping Raft groups for %d partitions across multiple brokers", len(assignments))

	var errors []error
	stoppedGroups := 0

	for _, assignment := range assignments {
		err := pa.stopSinglePartitionRaftGroup(assignment)
		if err != nil {
			log.Printf("Failed to stop Raft group for partition %s-%d: %v",
				assignment.TopicName, assignment.PartitionID, err)
			errors = append(errors, err)
		} else {
			stoppedGroups++
		}
	}

	if len(errors) > 0 {
		log.Printf("Successfully stopped %d/%d Raft groups, %d failed",
			stoppedGroups, len(assignments), len(errors))
		return fmt.Errorf("failed to stop %d Raft groups: %v", len(errors), errors[0])
	}

	log.Printf("Successfully stopped all %d partition Raft groups", len(assignments))
	return nil
}

// stopSinglePartitionRaftGroup coordinates the shutdown of a single partition Raft group
func (pa *PartitionAssigner) stopSinglePartitionRaftGroup(assignment *PartitionAssignment) error {
	log.Printf("Stopping Raft group %d for partition %s-%d with replicas %v",
		assignment.RaftGroupID, assignment.TopicName, assignment.PartitionID, assignment.Replicas)

	// Stop Raft group on each broker
	for _, brokerID := range assignment.Replicas {
		err := pa.stopRaftGroupOnBroker(assignment, brokerID)
		if err != nil {
			return err
		}
	}

	return nil
}

// stopRaftGroupOnBroker stops the Raft group on a specific broker
func (pa *PartitionAssigner) stopRaftGroupOnBroker(assignment *PartitionAssignment, brokerID string) error {
	log.Printf("Stopping Raft group %d on broker %s", assignment.RaftGroupID, brokerID)

	if brokerID == pa.getCurrentBrokerID() {
		return pa.stopRaftGroupLocally(assignment)
	} else {
		return pa.sendStopRaftGroupCommand(assignment, brokerID)
	}
}

// stopRaftGroupLocally stops the Raft group on the current broker
func (pa *PartitionAssigner) stopRaftGroupLocally(assignment *PartitionAssignment) error {
	log.Printf("Stopping Raft group %d locally", assignment.RaftGroupID)

	err := pa.raftManager.StopRaftGroup(assignment.RaftGroupID)
	if err != nil {
		return fmt.Errorf("failed to stop Raft group %d locally: %w", assignment.RaftGroupID, err)
	}

	log.Printf("Successfully stopped Raft group %d locally", assignment.RaftGroupID)
	return nil
}

// sendStopRaftGroupCommand sends a command to a remote broker to stop its Raft group
func (pa *PartitionAssigner) sendStopRaftGroupCommand(assignment *PartitionAssignment, brokerID string) error {
	broker, exists := pa.metadata.Brokers[brokerID]
	if !exists {
		return fmt.Errorf("broker %s not found in metadata", brokerID)
	}

	log.Printf("Sending StopRaftGroup command to broker %s:%d for group %d",
		broker.Address, broker.Port, assignment.RaftGroupID)

	request := &StopPartitionRaftGroupRequest{
		RaftGroupID: assignment.RaftGroupID,
		TopicName:   assignment.TopicName,
		PartitionID: assignment.PartitionID,
	}

	resp, err := pa.sendStopPartitionRaftGroupRequest(broker.Address, broker.Port, request)
	if err != nil {
		return fmt.Errorf("failed to send stop request to broker %s:%d: %w",
			broker.Address, broker.Port, err)
	}

	if resp.Success {
		log.Printf("Successfully sent StopPartitionRaftGroup request to broker %s:%d",
			broker.Address, broker.Port)
	} else {
		return fmt.Errorf("failed to stop Raft group on broker %s:%d: %s",
			broker.Address, broker.Port, resp.Error)
	}

	return nil
}
