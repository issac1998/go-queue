package metadata

import (
	"fmt"
	"sync"
	"time"
)

// ConsumerGroup 消费者组
type ConsumerGroup struct {
	ID         string               `json:"id"`         // 消费者组ID
	Members    map[string]*Consumer `json:"members"`    // 组内消费者
	Partitions map[string][]int32   `json:"partitions"` // Topic -> 分区列表
	State      GroupState           `json:"state"`      // 组状态
	Protocol   string               `json:"protocol"`   // 分区分配协议
	Leader     string               `json:"leader"`     // 组Leader ID
	Generation int32                `json:"generation"` // 组世代
	CreatedAt  time.Time            `json:"created_at"`
	UpdatedAt  time.Time            `json:"updated_at"`
	mu         sync.RWMutex
}

// Consumer 消费者
type Consumer struct {
	ID             string             `json:"id"`              // 消费者ID
	ClientID       string             `json:"client_id"`       // 客户端ID
	GroupID        string             `json:"group_id"`        // 所属消费者组
	Subscriptions  []string           `json:"subscriptions"`   // 订阅的Topic列表
	Assignment     map[string][]int32 `json:"assignment"`      // 分配的分区 Topic -> 分区列表
	LastHeartbeat  time.Time          `json:"last_heartbeat"`  // 最后心跳时间
	SessionTimeout time.Duration      `json:"session_timeout"` // 会话超时时间
	JoinedAt       time.Time          `json:"joined_at"`
	mu             sync.RWMutex
}

// GroupState 消费者组状态
type GroupState int32

const (
	GroupStateEmpty               GroupState = iota // 空组
	GroupStateStable                                // 稳定状态
	GroupStatePreparingRebalance                    // 准备重平衡
	GroupStateCompletingRebalance                   // 完成重平衡
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

// OffsetCommit 提交的offset信息
type OffsetCommit struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Metadata  string    `json:"metadata"`
	Timestamp time.Time `json:"timestamp"`
}

// ConsumerGroupManager 消费者组管理器
type ConsumerGroupManager struct {
	groups  map[string]*ConsumerGroup // 消费者组
	offsets map[string]*OffsetStorage // offset存储 groupID -> OffsetStorage
	mu      sync.RWMutex
}

// OffsetStorage offset存储
type OffsetStorage struct {
	GroupID string                             `json:"group_id"`
	Offsets map[string]map[int32]*OffsetCommit `json:"offsets"` // Topic -> Partition -> OffsetCommit
	mu      sync.RWMutex
}

// NewConsumerGroupManager 创建消费者组管理器
func NewConsumerGroupManager() *ConsumerGroupManager {
	return &ConsumerGroupManager{
		groups:  make(map[string]*ConsumerGroup),
		offsets: make(map[string]*OffsetStorage),
	}
}

// CreateGroup 创建消费者组
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
		Protocol:   "round-robin", // 默认使用轮询分配
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

// GetGroup 获取消费者组
func (cgm *ConsumerGroupManager) GetGroup(groupID string) (*ConsumerGroup, bool) {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	group, exists := cgm.groups[groupID]
	return group, exists
}

// DeleteGroup 删除消费者组
func (cgm *ConsumerGroupManager) DeleteGroup(groupID string) error {
	cgm.mu.Lock()
	defer cgm.mu.Unlock()

	delete(cgm.groups, groupID)
	delete(cgm.offsets, groupID)
	return nil
}

// JoinGroup 加入消费者组
func (cgm *ConsumerGroupManager) JoinGroup(groupID, consumerID, clientID string, topics []string, sessionTimeout time.Duration) (*Consumer, error) {
	group := cgm.CreateGroup(groupID)

	group.mu.Lock()
	defer group.mu.Unlock()

	// 创建消费者
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

	// 添加到组中
	group.Members[consumerID] = consumer
	group.UpdatedAt = time.Now()

	// 如果是第一个成员，设为Leader
	if len(group.Members) == 1 {
		group.Leader = consumerID
	}

	// 状态变更：触发重平衡
	if group.State == GroupStateStable {
		group.State = GroupStatePreparingRebalance
		group.Generation++
	} else if group.State == GroupStateEmpty {
		group.State = GroupStatePreparingRebalance
		group.Generation = 1
	}

	return consumer, nil
}

// LeaveGroup 离开消费者组
func (cgm *ConsumerGroupManager) LeaveGroup(groupID, consumerID string) error {
	group, exists := cgm.GetGroup(groupID)
	if !exists {
		return fmt.Errorf("group %s not found", groupID)
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.Members, consumerID)
	group.UpdatedAt = time.Now()

	// 如果删除的是Leader，重新选举
	if group.Leader == consumerID && len(group.Members) > 0 {
		for memberID := range group.Members {
			group.Leader = memberID
			break
		}
	}

	// 状态变更
	if len(group.Members) == 0 {
		group.State = GroupStateEmpty
	} else {
		group.State = GroupStatePreparingRebalance
		group.Generation++
	}

	return nil
}

// Heartbeat 心跳
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

// CommitOffset 提交offset
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

// GetCommittedOffset 获取已提交的offset
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

// RebalancePartitions 重平衡分区分配
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

	// 使用轮询算法分配分区
	assignment := cgm.roundRobinAssignment(group.Members, topicPartitions)

	// 更新分配结果
	for consumerID, consumer := range group.Members {
		consumer.Assignment = assignment[consumerID]
	}

	group.Partitions = topicPartitions
	group.State = GroupStateStable
	group.UpdatedAt = time.Now()

	return nil
}

// roundRobinAssignment 轮询分区分配算法
func (cgm *ConsumerGroupManager) roundRobinAssignment(members map[string]*Consumer, topicPartitions map[string][]int32) map[string]map[string][]int32 {
	assignment := make(map[string]map[string][]int32)

	// 初始化分配结果
	for consumerID := range members {
		assignment[consumerID] = make(map[string][]int32)
	}

	if len(members) == 0 {
		return assignment
	}

	// 获取消费者ID列表并排序（保证一致性）
	var consumerIDs []string
	for consumerID := range members {
		consumerIDs = append(consumerIDs, consumerID)
	}

	// 简单排序
	for i := 0; i < len(consumerIDs)-1; i++ {
		for j := i + 1; j < len(consumerIDs); j++ {
			if consumerIDs[i] > consumerIDs[j] {
				consumerIDs[i], consumerIDs[j] = consumerIDs[j], consumerIDs[i]
			}
		}
	}

	// 轮询分配分区
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

// CleanupExpiredConsumers 清理过期的消费者
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

		// 移除过期消费者
		for _, consumerID := range expiredConsumers {
			delete(group.Members, consumerID)
		}

		// 更新组状态
		if len(expiredConsumers) > 0 {
			group.UpdatedAt = now
			if len(group.Members) == 0 {
				group.State = GroupStateEmpty
			} else {
				group.State = GroupStatePreparingRebalance
				group.Generation++

				// 重新选举Leader
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

		// 如果组为空且超过一定时间，可以考虑删除组
		if group.State == GroupStateEmpty && now.Sub(group.UpdatedAt) > time.Hour {
			delete(cgm.groups, groupID)
			delete(cgm.offsets, groupID)
		}
	}
}

// ListGroups 列出所有消费者组
func (cgm *ConsumerGroupManager) ListGroups() []*ConsumerGroup {
	cgm.mu.RLock()
	defer cgm.mu.RUnlock()

	groups := make([]*ConsumerGroup, 0, len(cgm.groups))
	for _, group := range cgm.groups {
		groups = append(groups, group)
	}
	return groups
}

// GetGroupState 获取消费者组状态
func (cgm *ConsumerGroupManager) GetGroupState(groupID string) (GroupState, error) {
	group, exists := cgm.GetGroup(groupID)
	if !exists {
		return GroupStateEmpty, fmt.Errorf("group %s not found", groupID)
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	return group.State, nil
}

// GetGroupInfo 获取消费者组信息（带锁保护）
func (cgm *ConsumerGroupManager) GetGroupInfo(groupID string) (*ConsumerGroup, bool, func()) {
	group, exists := cgm.GetGroup(groupID)
	if !exists {
		return nil, false, nil
	}

	group.mu.RLock()
	return group, true, func() { group.mu.RUnlock() }
}
