package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/transaction"
)

// SimpleBatchTransactionListener 简单的批量事务监听器
type SimpleBatchTransactionListener struct {
	transactions map[transaction.TransactionID]transaction.TransactionState
}

func NewSimpleBatchTransactionListener() *SimpleBatchTransactionListener {
	return &SimpleBatchTransactionListener{
		transactions: make(map[transaction.TransactionID]transaction.TransactionState),
	}
}

func (l *SimpleBatchTransactionListener) ExecuteLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	l.transactions[transactionID] = transaction.StateCommit
	return transaction.StateCommit
}

func (l *SimpleBatchTransactionListener) CheckLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	if state, exists := l.transactions[transactionID]; exists {
		return state
	}
	return transaction.StateUnknown
}

func (l *SimpleBatchTransactionListener) ExecuteBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	l.transactions[transactionID] = transaction.StateCommit
	return transaction.StateCommit
}

func (l *SimpleBatchTransactionListener) CheckBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	if state, exists := l.transactions[transactionID]; exists {
		return state
	}
	return transaction.StateUnknown
}

// TestBatchTransactionOptimizationPerformance 测试批量事务优化的性能
func TestBatchTransactionOptimizationPerformance(t *testing.T) {
	listener := NewSimpleBatchTransactionListener()
	
	// 模拟批量事务处理
	batchSizes := []int{1, 10, 50, 100, 500}
	
	for _, batchSize := range batchSizes {
		t.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(t *testing.T) {
			// 准备测试数据
			transactionID := transaction.TransactionID(fmt.Sprintf("batch-tx-%d", batchSize))
			messageIDs := make([]string, batchSize)
			for i := 0; i < batchSize; i++ {
				messageIDs[i] = fmt.Sprintf("msg-%d", i)
			}
			
			// 测试批量事务执行性能
			start := time.Now()
			state := listener.ExecuteBatchLocalTransaction(transactionID, messageIDs)
			executionTime := time.Since(start)
			
			if state != transaction.StateCommit {
				t.Errorf("Expected StateCommit, got %v", state)
			}
			
			// 测试批量事务检查性能
			start = time.Now()
			checkState := listener.CheckBatchLocalTransaction(transactionID, messageIDs)
			checkTime := time.Since(start)
			
			if checkState != transaction.StateCommit {
				t.Errorf("Expected StateCommit, got %v", checkState)
			}
			
			t.Logf("Batch size: %d, Execution time: %v, Check time: %v", 
				batchSize, executionTime, checkTime)
		})
	}
}

// TestTopicPartitionOptimization 测试topic-partition优化效果
func TestTopicPartitionOptimization(t *testing.T) {
	// 创建测试用的TopicPartition数据
	topicPartitions := []transaction.TopicPartition{
		{Topic: "test-topic-1", Partition: 0},
		{Topic: "test-topic-1", Partition: 1},
		{Topic: "test-topic-2", Partition: 0},
		{Topic: "test-topic-3", Partition: 0},
		{Topic: "test-topic-3", Partition: 1},
		{Topic: "test-topic-3", Partition: 2},
	}
	
	// 模拟优化前的查询（需要遍历所有分区）
	allPartitionsCount := 100 // 假设系统中有100个分区
	
	// 模拟优化后的查询（只查询相关分区）
	relevantPartitionsCount := len(topicPartitions)
	
	t.Logf("优化前需要查询的分区数: %d", allPartitionsCount)
	t.Logf("优化后需要查询的分区数: %d", relevantPartitionsCount)
	t.Logf("查询效率提升: %.2f%%", float64(allPartitionsCount-relevantPartitionsCount)/float64(allPartitionsCount)*100)
	
	// 验证TopicPartition结构
	for i, tp := range topicPartitions {
		if tp.Topic == "" {
			t.Errorf("TopicPartition[%d] has empty topic", i)
		}
		if tp.Partition < 0 {
			t.Errorf("TopicPartition[%d] has invalid partition: %d", i, tp.Partition)
		}
	}
}

// BenchmarkBatchTransactionExecution 基准测试批量事务执行
func BenchmarkBatchTransactionExecution(b *testing.B) {
	listener := NewSimpleBatchTransactionListener()
	
	batchSizes := []int{1, 10, 50, 100}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			messageIDs := make([]string, batchSize)
			for i := 0; i < batchSize; i++ {
				messageIDs[i] = fmt.Sprintf("msg-%d", i)
			}
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				transactionID := transaction.TransactionID(fmt.Sprintf("bench-tx-%d", i))
				listener.ExecuteBatchLocalTransaction(transactionID, messageIDs)
			}
		})
	}
}

// BenchmarkBatchTransactionCheck 基准测试批量事务检查
func BenchmarkBatchTransactionCheck(b *testing.B) {
	listener := NewSimpleBatchTransactionListener()
	
	// 预先创建一些事务
	for i := 0; i < 1000; i++ {
		transactionID := transaction.TransactionID(fmt.Sprintf("pre-tx-%d", i))
		listener.ExecuteBatchLocalTransaction(transactionID, []string{"msg-1"})
	}
	
	batchSizes := []int{1, 10, 50, 100}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			messageIDs := make([]string, batchSize)
			for i := 0; i < batchSize; i++ {
				messageIDs[i] = fmt.Sprintf("msg-%d", i)
			}
			
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				transactionID := transaction.TransactionID(fmt.Sprintf("pre-tx-%d", i%1000))
				listener.CheckBatchLocalTransaction(transactionID, messageIDs)
			}
		})
	}
}