package transaction

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// TransactionRouter 负责根据事务ID路由事务操作到对应的分区Raft组
type TransactionRouter struct {
	// 可以添加缓存或其他优化
}

// NewTransactionRouter 创建新的事务路由器
func NewTransactionRouter() *TransactionRouter {
	return &TransactionRouter{}
}

// ParseTransactionID 解析事务ID，提取分区信息
type TransactionIDInfo struct {
	BrokerID    string
	TopicHash   uint32
	PartitionID int32
	Timestamp   int64
	Sequence    uint32
}

// ParseTransactionID 解析事务ID格式: txn_<brokerID>_<topicHash>_<partitionID>_<timestamp>_<sequence>
func (tr *TransactionRouter) ParseTransactionID(txnID TransactionID) (*TransactionIDInfo, error) {
	idStr := string(txnID)
	parts := strings.Split(idStr, "_")

	if len(parts) != 6 || parts[0] != "txn" {
		return nil, fmt.Errorf("invalid transaction ID format: %s", txnID)
	}

	brokerID := parts[1]

	topicHash, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid topic hash in transaction ID: %s", parts[2])
	}

	partitionID, err := strconv.ParseInt(parts[3], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid partition ID in transaction ID: %s", parts[3])
	}

	timestamp, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp in transaction ID: %s", parts[4])
	}

	sequence, err := strconv.ParseUint(parts[5], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid sequence in transaction ID: %s", parts[5])
	}

	return &TransactionIDInfo{
		BrokerID:    brokerID,
		TopicHash:   uint32(topicHash),
		PartitionID: int32(partitionID),
		Timestamp:   timestamp,
		Sequence:    uint32(sequence),
	}, nil
}

// GenerateRaftGroupID 根据topic名称和分区ID生成Raft组ID
// 使用与PartitionAssigner相同的算法
func (tr *TransactionRouter) GenerateRaftGroupID(topicName string, partitionID int32) uint64 {
	h := fnv.New64a()
	h.Write([]byte(fmt.Sprintf("%s-%d", topicName, partitionID)))
	return h.Sum64()
}

// GetTargetRaftGroupID 根据事务ID和topic名称获取目标Raft组ID
func (tr *TransactionRouter) GetTargetRaftGroupID(txnID TransactionID, topicName string) (uint64, error) {
	txnInfo, err := tr.ParseTransactionID(txnID)
	if err != nil {
		return 0, err
	}

	// 验证topic hash是否匹配
	h := fnv.New32a()
	h.Write([]byte(topicName))
	expectedTopicHash := h.Sum32()

	if txnInfo.TopicHash != expectedTopicHash {
		return 0, fmt.Errorf("topic hash mismatch: expected %d, got %d", expectedTopicHash, txnInfo.TopicHash)
	}

	// 生成Raft组ID
	return tr.GenerateRaftGroupID(topicName, txnInfo.PartitionID), nil
}

// GetPartitionID 从事务ID中提取分区ID
func (tr *TransactionRouter) GetPartitionID(txnID TransactionID) (int32, error) {
	txnInfo, err := tr.ParseTransactionID(txnID)
	if err != nil {
		return 0, err
	}
	return txnInfo.PartitionID, nil
}

// ValidateTransactionID 验证事务ID格式是否正确
func (tr *TransactionRouter) ValidateTransactionID(txnID TransactionID) error {
	_, err := tr.ParseTransactionID(txnID)
	return err
}
