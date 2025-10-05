package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BatchMockTransactionListener 支持批量事务的模拟监听器
type BatchMockTransactionListener struct {
	mu                   sync.RWMutex
	transactions         map[transaction.TransactionID]transaction.TransactionState
	batchTransactions    map[transaction.TransactionID][]transaction.HalfMessage
	callbackCount        int
	batchCallbackCount   int
	shouldFailBatch      bool
	failAtMessageIndex   int
}

func NewBatchMockTransactionListener() *BatchMockTransactionListener {
	return &BatchMockTransactionListener{
		transactions:      make(map[transaction.TransactionID]transaction.TransactionState),
		batchTransactions: make(map[transaction.TransactionID][]transaction.HalfMessage),
		failAtMessageIndex: -1,
	}
}

// 实现单消息事务方法（保持向后兼容）
func (m *BatchMockTransactionListener) ExecuteLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 模拟本地事务执行
	state := transaction.StateCommit
	if string(transactionID) == "rollback-tx" {
		state = transaction.StateRollback
	} else if string(transactionID) == "unknown-tx" {
		state = transaction.StateUnknown
	}
	
	m.transactions[transactionID] = state
	return state
}

func (m *BatchMockTransactionListener) CheckLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.callbackCount++
	
	if state, exists := m.transactions[transactionID]; exists {
		return state
	}
	
	return transaction.StateUnknown
}

// 实现批量事务方法
func (m *BatchMockTransactionListener) ExecuteBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 模拟批量事务失败场景
	if m.shouldFailBatch {
		m.transactions[transactionID] = transaction.StateRollback
		return transaction.StateRollback
	}
	
	// 模拟在特定消息索引处失败
	if m.failAtMessageIndex >= 0 && m.failAtMessageIndex < len(messageIDs) {
		m.transactions[transactionID] = transaction.StateRollback
		return transaction.StateRollback
	}
	
	// 模拟批量事务成功
	state := transaction.StateCommit
	if string(transactionID) == "batch-rollback-tx" {
		state = transaction.StateRollback
	} else if string(transactionID) == "batch-unknown-tx" {
		state = transaction.StateUnknown
	}
	
	m.transactions[transactionID] = state
	return state
}

func (m *BatchMockTransactionListener) CheckBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.batchCallbackCount++
	
	if state, exists := m.transactions[transactionID]; exists {
		return state
	}
	
	return transaction.StateUnknown
}

// 辅助方法
func (m *BatchMockTransactionListener) SetShouldFailBatch(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFailBatch = fail
}

func (m *BatchMockTransactionListener) SetFailAtMessageIndex(index int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failAtMessageIndex = index
}

func (m *BatchMockTransactionListener) GetBatchMessages(txID transaction.TransactionID) []transaction.HalfMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.batchTransactions[txID]
}

func (m *BatchMockTransactionListener) GetCallbackCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.callbackCount
}

func (m *BatchMockTransactionListener) GetBatchCallbackCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.batchCallbackCount
}

// TestBatchTransactionSuccess 测试批量事务成功场景
func TestBatchTransactionSuccess(t *testing.T) {
	// 创建虚拟客户端
	c := &client.Client{}
	
	listener := NewBatchMockTransactionListener()
	producer := client.NewTransactionProducer(c, listener, "batch-test-producer")
	
	// 创建批量消息
	messages := []*client.TransactionMessage{
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("key-1"),
			Value:     []byte("value-1"),
			Headers:   map[string]string{"batch": "1"},
			Timeout:   5 * time.Second,
		},
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("key-2"),
			Value:     []byte("value-2"),
			Headers:   map[string]string{"batch": "2"},
			Timeout:   5 * time.Second,
		},
		{
			Topic:     "test-topic",
			Partition: 1,
			Key:       []byte("key-3"),
			Value:     []byte("value-3"),
			Headers:   map[string]string{"batch": "3"},
			Timeout:   5 * time.Second,
		},
	}
	
	// 执行批量事务
	txn, result, err := producer.SendBatchHalfMessagesAndDoLocal(messages)
	
	// 验证结果
	require.NoError(t, err)
	require.NotNil(t, txn)
	require.NotNil(t, result)
	assert.NoError(t, result.Error)
	assert.NotEmpty(t, result.TransactionID)
	
	// 验证批量消息被正确存储
	batchMessages := listener.GetBatchMessages(result.TransactionID)
	assert.Len(t, batchMessages, 3)
	
	// 验证每个消息的内容
	for i, msg := range batchMessages {
		assert.Equal(t, result.TransactionID, msg.TransactionID)
		assert.Equal(t, messages[i].Topic, msg.Topic)
		assert.Equal(t, messages[i].Partition, msg.Partition)
		assert.Equal(t, messages[i].Key, msg.Key)
		assert.Equal(t, messages[i].Value, msg.Value)
		assert.Equal(t, messages[i].Headers, msg.Headers)
		assert.Equal(t, transaction.StatePrepared, msg.State)
	}
}

// TestBatchTransactionRollback 测试批量事务回滚场景
func TestBatchTransactionRollback(t *testing.T) {
	c := &client.Client{}
	
	listener := NewBatchMockTransactionListener()
	listener.SetShouldFailBatch(true) // 设置批量事务失败
	
	producer := client.NewTransactionProducer(c, listener, "batch-rollback-producer")
	
	messages := []*client.TransactionMessage{
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("rollback-key-1"),
			Value:     []byte("rollback-value-1"),
			Timeout:   5 * time.Second,
		},
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("rollback-key-2"),
			Value:     []byte("rollback-value-2"),
			Timeout:   5 * time.Second,
		},
	}
	
	// 执行批量事务
	txn, result, err := producer.SendBatchHalfMessagesAndDoLocal(messages)
	
	// 验证结果
	require.NoError(t, err)
	require.NotNil(t, txn)
	require.NotNil(t, result)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "batch transaction rolled back")
	
	// 验证批量消息仍被存储（用于回滚处理）
	batchMessages := listener.GetBatchMessages(result.TransactionID)
	assert.Len(t, batchMessages, 2)
}

// TestBatchTransactionEmpty 测试空批量消息
func TestBatchTransactionEmpty(t *testing.T) {
	c := &client.Client{}
	
	listener := NewBatchMockTransactionListener()
	producer := client.NewTransactionProducer(c, listener, "empty-batch-producer")
	
	// 执行空批量事务
	txn, result, err := producer.SendBatchHalfMessagesAndDoLocal([]*client.TransactionMessage{})
	
	// 验证结果
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no messages provided for batch transaction")
	assert.Nil(t, txn)
	assert.Nil(t, result)
}

// TestBatchTransactionPartialFailure 测试批量事务部分失败场景
func TestBatchTransactionPartialFailure(t *testing.T) {
	c := &client.Client{}
	
	listener := NewBatchMockTransactionListener()
	listener.SetFailAtMessageIndex(1) // 在第二个消息处失败
	
	producer := client.NewTransactionProducer(c, listener, "partial-fail-producer")
	
	messages := []*client.TransactionMessage{
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("partial-key-1"),
			Value:     []byte("partial-value-1"),
			Timeout:   5 * time.Second,
		},
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("partial-key-2"),
			Value:     []byte("partial-value-2"),
			Timeout:   5 * time.Second,
		},
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("partial-key-3"),
			Value:     []byte("partial-value-3"),
			Timeout:   5 * time.Second,
		},
	}
	
	// 执行批量事务
	txn, result, err := producer.SendBatchHalfMessagesAndDoLocal(messages)
	
	// 验证结果
	require.NoError(t, err)
	require.NotNil(t, txn)
	require.NotNil(t, result)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "batch transaction rolled back")
	
	// 验证所有消息都被处理（即使在中间失败）
	batchMessages := listener.GetBatchMessages(result.TransactionID)
	assert.Len(t, batchMessages, 3)
}

// TestBatchTransactionConcurrency 测试批量事务并发场景
func TestBatchTransactionConcurrency(t *testing.T) {
	c := &client.Client{}
	
	listener := NewBatchMockTransactionListener()
	producer := client.NewTransactionProducer(c, listener, "concurrent-batch-producer")
	
	concurrency := 5
	messagesPerBatch := 3
	var wg sync.WaitGroup
	results := make([]*client.TransactionResult, concurrency)
	errors := make([]error, concurrency)
	
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			// 为每个并发创建不同的批量消息
			messages := make([]*client.TransactionMessage, messagesPerBatch)
			for j := 0; j < messagesPerBatch; j++ {
				messages[j] = &client.TransactionMessage{
					Topic:     "concurrent-topic",
					Partition: int32(index % 2),
					Key:       []byte(fmt.Sprintf("concurrent-key-%d-%d", index, j)),
					Value:     []byte(fmt.Sprintf("concurrent-value-%d-%d", index, j)),
					Headers:   map[string]string{"batch": fmt.Sprintf("%d", index), "msg": fmt.Sprintf("%d", j)},
					Timeout:   5 * time.Second,
				}
			}
			
			// 执行批量事务
			_, result, err := producer.SendBatchHalfMessagesAndDoLocal(messages)
			results[index] = result
			errors[index] = err
		}(i)
	}
	
	wg.Wait()
	
	// 验证所有并发批量事务都成功
	for i := 0; i < concurrency; i++ {
		assert.NoError(t, errors[i], "Batch transaction %d should succeed", i)
		assert.NotNil(t, results[i], "Result %d should not be nil", i)
		if results[i] != nil {
			assert.NoError(t, results[i].Error, "Batch transaction %d should not have error", i)
		}
	}
	
	// 验证所有批量消息都被正确处理
	for i := 0; i < concurrency; i++ {
		if results[i] != nil {
			batchMessages := listener.GetBatchMessages(results[i].TransactionID)
			assert.Len(t, batchMessages, messagesPerBatch, "Batch %d should have %d messages", i, messagesPerBatch)
		}
	}
}

// TestBatchVsSingleTransactionPerformance 比较批量事务与单个事务的性能
func TestBatchVsSingleTransactionPerformance(t *testing.T) {
	c := &client.Client{}
	
	listener := NewBatchMockTransactionListener()
	producer := client.NewTransactionProducer(c, listener, "performance-test-producer")
	
	messageCount := 10
	
	// 创建测试消息
	messages := make([]*client.TransactionMessage, messageCount)
	for i := 0; i < messageCount; i++ {
		messages[i] = &client.TransactionMessage{
			Topic:     "perf-topic",
			Partition: 0,
			Key:       []byte(fmt.Sprintf("perf-key-%d", i)),
			Value:     []byte(fmt.Sprintf("perf-value-%d", i)),
			Timeout:   5 * time.Second,
		}
	}
	
	// 测试单个事务性能
	start := time.Now()
	for _, msg := range messages {
		_, _, err := producer.SendHalfMessageAndDoLocal(msg)
		require.NoError(t, err)
	}
	singleTxnDuration := time.Since(start)
	
	// 测试批量事务性能
	start = time.Now()
	_, _, err := producer.SendBatchHalfMessagesAndDoLocal(messages)
	require.NoError(t, err)
	batchTxnDuration := time.Since(start)
	
	t.Logf("Single transactions duration: %v", singleTxnDuration)
	t.Logf("Batch transaction duration: %v", batchTxnDuration)
	
	// 批量事务应该更快（至少不会显著更慢）
	// 注意：在模拟环境中性能差异可能不明显，但在实际环境中批量事务会有显著优势
	assert.True(t, batchTxnDuration <= singleTxnDuration*2, 
		"Batch transaction should not be significantly slower than individual transactions")
}