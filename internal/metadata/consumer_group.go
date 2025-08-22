package metadata

import (
	"fmt"
	"sync"
	"time"
)

type ConsumerGroup struct {
	ID         string               `json:"id"`        
	Members    map[string]*Consumer `json:"members"`    
	Partitions map[string][]int32   `json:"partitions"` 
	State      GroupState           `json:"state"`      
	Leader     string               `json:"leader"`    
	Generation int32                `json:"generation"` 
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
	mu         sync.RWMutex
}

type Consumer struct {
	ID             string             `json:"id"`             
	ClientID       string             `json:"client_id"`       
	GroupID        string             `json:"group_id"`        
	Subscriptions  []string           `json:"subscriptions"`   
	Assignment     map[string][]int32 `json:"assignment"`      
	LastHeartbeat  time.Time          `json:"last_heartbeat"`  
	SessionTimeout time.Duration      `json:"session_timeout"` 
	JoinedAt       time.Time          `json:"joined_at"`
	mu             sync.RWMutex
}

type GroupState int32

const (
	GroupStateEmpty               GroupState = iota 
	GroupStateStable                               
	GroupStatePreparingRebalance                  
	GroupStateCompletingRebalance                   
)

func (s GroupState) String() string {
	switch s {
	case GroupStateEmpty:
		return "Empty"
	case GroupStateStable:
		return "Stable"
	case GroupStatePreparingRebalance:
		return "PreparingRebalance"
	case GroupStateCompletingRebalance:
		return "CompletingRebalance"
	default:
		return "Unknown"
	}
}

type OffsetCommit struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Metadata  string    `json:"metadata"`
	Timestamp time.Time `json:"timestamp"`
}

type ConsumerGroupManager struct {
	groups  map[string]*ConsumerGroup 
	offsets map[string]*OffsetStorage 
	mu      sync.RWMutex
}

type OffsetStorage struct {
	GroupID string                             `json:"group_id"`
	Offsets map[string]map[int32]*OffsetCommit `json:"offsets"` // Topic -> Partition -> OffsetCommit
	mu      sync.RWMutex
}

func NewConsumerGroupManager() *ConsumerGroupManager {
	return &ConsumerGroupManager{
		groups:  make(map[string]*ConsumerGroup),
		offsets: make(map[string]*OffsetStorage),
	}
}

func (cgm *ConsumerGroupManager) CreateGroup(groupID string) *ConsumerGroup {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	if group, exists := cgm.groups[groupID]; exists {
		return group
	}

	group := &ConsumerGroup{
		ID:         groupID,
		Members:    make(map[string]*Consumer),
		Partitions: make(map[string][]int32),
		State:      GroupStateEmpty,
		Generation: 0,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	cgm.groups[groupID] = group
	cgm.offsets[groupID] = &OffsetStorage{
		GroupID: groupID,
		Offsets: make(map[string]map[int32]*OffsetCommit),
	}

	return group
}

func (cgm *ConsumerGroupManager) GetGroup(groupID string) (*ConsumerGroup, bool) {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	group, exists := cgm.groups[groupID]
	return group, exists
}

func (cgm *ConsumerGroupManager) DeleteGroup(groupID string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	delete(cgm.groups, groupID)
	delete(cgm.offsets, groupID)
	return nil
}

func (cgm *ConsumerGroupManager) JoinGroup(groupID, consumerID, clientID string, topics []string, sessionTimeout time.Duration) (*Consumer, error) {
	group := cgm.CreateGroup(groupID)

	group.mu.Lock()
	defer group.mu.Unlock()

	
	consumer := &Consumer{
		ID:             consumerID,
		ClientID:       clientID,
		GroupID:        groupID,
		Subscriptions:  topics,
		Assignment:     make(map[string][]int32),
		LastHeartbeat:  time.Now(),
		SessionTimeout: sessionTimeout,
		JoinedAt:       time.Now(),
	}

	group.Members[consumerID] = consumer
	group.UpdatedAt = time.Now()

	if len(group.Members) == 1 {
		group.Leader = consumerID
	}

	// trigger rebalance
	if group.State == GroupStateStable {
		group.State = GroupStatePreparingRebalance
		group.Generation++
	} else if group.State == GroupStateEmpty {
		group.State = GroupStatePreparingRebalance
		group.Generation = 1
	}

	return consumer, nil
}

// LeaveGroup leave group
func (cgm *ConsumerGroupManager) LeaveGroup(groupID, consumerID string) error {
	group, exists := cgm.GetGroup(groupID)
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.Members, consumerID)
	group.UpdatedAt = time.Now()

	if group.Leader == consumerID && len(group.Members) > 0 {
		for memberID := range group.Members {
			group.Leader = memberID
			break
		}
	}

	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
	} else {
		group.State = GroupStatePreparingRebalance
		group.Generation++
	}

	return nil
}

// Heartbeat 
func (cgm *ConsumerGroupManager) Heartbeat(groupID, consumerID string) error {
	group, exists := cgm.GetGroup(groupID)
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	consumer, exists := group.Members[consumerID]
	if !exists {
		return fmt.Errorf("consumer %s not found in group %s", consumerID, groupID)
	}

	consumer.LastHeartbeat = time.Now()
	return nil
}

func (cgm *ConsumerGroupManager) CommitOffset(groupID, topic string, partition int32, offset int64, metadata string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	offsetStorage, exists := cgm.offsets[groupID]
	if !exists {
		return fmt.Errorf("offset storage for group %s not found", groupID)
	}

	offsetStorage.mu.Lock()
	defer offsetStorage.mu.Unlock()

	if offsetStorage.Offsets[topic] == nil {
		offsetStorage.Offsets[topic] = make(map[int32]*OffsetCommit)
	}

	offsetStorage.Offsets[topic][partition] = &OffsetCommit{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}

	return nil
}

func (cgm *ConsumerGroupManager) GetCommittedOffset(groupID, topic string, partition int32) (int64, bool) {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	offsetStorage, exists := cgm.offsets[groupID]
	if !exists {
		return -1, false
	}

	offsetStorage.mu.RLock()
	defer offsetStorage.mu.RUnlock()

	if offsetStorage.Offsets[topic] == nil {
		return -1, false
	}

	offsetCommit, exists := offsetStorage.Offsets[topic][partition]
	if !exists {
		return -1, false
	}

	return offsetCommit.Offset, true
}

func (cgm *ConsumerGroupManager) RebalancePartitions(groupID string, topicPartitions map[string][]int32) error {
	group, exists := cgm.GetGroup(groupID)
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if group.State != GroupStatePreparingRebalance {
		return fmt.Errorf("group %s is not in preparing rebalance state", groupID)
	}

	
	assignment := cgm.roundRobinAssignment(group.Members, topicPartitions)

	
	for consumerID, consumer := range group.Members {
		consumer.Assignment = assignment[consumerID]
	}

	group.Partitions = topicPartitions
	group.State = GroupStateStable
	group.UpdatedAt = time.Now()

	return nil
}

func (cgm *ConsumerGroupManager) roundRobinAssignment(members map[string]*Consumer, topicPartitions map[string][]int32) map[string]map[string][]int32 {
	assignment := make(map[string]map[string][]int32)

	
	for consumerID := range members {
		assignment[consumerID] = make(map[string][]int32)
	}

	if len(members) == 0 {
		return assignment
	}

	
	var consumerIDs []string
	for consumerID := range members {
		consumerIDs = append(consumerIDs, consumerID)
	}

	
	for i := 0; i < len(consumerIDs)-1; i++ {
		for j := i + 1; j < len(consumerIDs); j++ {
			if consumerIDs[i] > consumerIDs[j] {
				consumerIDs[i], consumerIDs[j] = consumerIDs[j], consumerIDs[i]
			}
		}
	}

	
	consumerIndex := 0
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			consumerID := consumerIDs[consumerIndex]
			if assignment[consumerID][topic] == nil {
				assignment[consumerID][topic] = make([]int32, 0)
			}
			assignment[consumerID][topic] = append(assignment[consumerID][topic], partition)
			consumerIndex = (consumerIndex + 1) % len(consumerIDs)
		}
	}

	return assignment
}

// CleanupExpiredConsumers clean expired consumers
func (cgm *ConsumerGroupManager) CleanupExpiredConsumers() {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	now := time.Now()
	for groupID, group := range cgm.groups {
		group.mu.Lock()

		var expiredConsumers []string
		for consumerID, consumer := range group.Members {
			if now.Sub(consumer.LastHeartbeat) > consumer.SessionTimeout {
				expiredConsumers = append(expiredConsumers, consumerID)
			}
		}

		for _, consumerID := range expiredConsumers {
			delete(group.Members, consumerID)
		}

		if len(expiredConsumers) > 0 {
			group.UpdatedAt = now
			if len(group.Members) == 0 {
				group.State = GroupStateEmpty
			} else {
				group.State = GroupStatePreparingRebalance
				group.Generation++

				
				if group.Leader != "" {
					found := false
					for memberID := range group.Members {
						if memberID == group.Leader {
							found = true
							break
						}
					}
					if !found {
						for memberID := range group.Members {
							group.Leader = memberID
							break
						}
					}
				}
			}
		}

		group.mu.Unlock()

		
		if group.State == GroupStateEmpty && now.Sub(group.UpdatedAt) > time.Hour {
			delete(cgm.groups, groupID)
			delete(cgm.offsets, groupID)
		}
	}
}

func (cgm *ConsumerGroupManager) GetGroupInfo(groupID string) (*ConsumerGroup, bool, func()) {
	group, exists := cgm.GetGroup(groupID)
	if !exists {
		return nil, false, nil
	}

	group.mu.RLock()
	return group, true, func() { group.mu.RUnlock() }
}
