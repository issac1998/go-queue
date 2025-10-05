package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/transaction"
	"github.com/stretchr/testify/assert"
)



// SimpleMockTransactionListener 简单的模拟事务监听器，用于TCP回调测试
type SimpleMockTransactionListener struct {
	mu            sync.RWMutex
	transactions  map[transaction.TransactionID]transaction.TransactionState
	callbackCount int
}

func NewSimpleMockTransactionListener() *SimpleMockTransactionListener {
	return &SimpleMockTransactionListener{
		transactions: make(map[transaction.TransactionID]transaction.TransactionState),
	}
}

func (m *SimpleMockTransactionListener) ExecuteLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
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

func (m *SimpleMockTransactionListener) CheckLocalTransaction(transactionID transaction.TransactionID, messageID string) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.callbackCount++
	
	// 返回之前执行的事务状态
	if state, exists := m.transactions[transactionID]; exists {
		return state
	}
	
	return transaction.StateUnknown
}

// ExecuteBatchLocalTransaction 执行批量本地事务
func (m *SimpleMockTransactionListener) ExecuteBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 模拟批量本地事务执行
	state := transaction.StateCommit
	if string(transactionID) == "rollback-tx" {
		state = transaction.StateRollback
	} else if string(transactionID) == "unknown-tx" {
		state = transaction.StateUnknown
	}
	
	m.transactions[transactionID] = state
	return state
}

// CheckBatchLocalTransaction 检查批量本地事务状态
func (m *SimpleMockTransactionListener) CheckBatchLocalTransaction(transactionID transaction.TransactionID, messageIDs []string) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.callbackCount++
	
	// 返回之前执行的批量事务状态
	if state, exists := m.transactions[transactionID]; exists {
		return state
	}
	
	return transaction.StateUnknown
}

func (m *SimpleMockTransactionListener) GetCallbackCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.callbackCount
}

func (m *SimpleMockTransactionListener) GetTransactionState(txID transaction.TransactionID) transaction.TransactionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if state, exists := m.transactions[txID]; exists {
		return state
	}
	return transaction.StateUnknown
}

// TestTCPCallbackWithTransactionProducer 测试使用TransactionProducer的TCP回调机制
func TestTCPCallbackWithTransactionProducer(t *testing.T) {
	// 创建一个虚拟客户端（不需要实际连接broker）
	c := &client.Client{}
	
	listener := NewSimpleMockTransactionListener()
	
	// 创建事务生产者
	producer := client.NewTransactionProducer(c, listener, "test-producer")
	
	// 启动TCP监听器
	port := "18080"
	err := producer.StartTransactionListener(port)
	assert.NoError(t, err)
	
	// 等待监听器启动
	time.Sleep(100 * time.Millisecond)
	defer producer.StopTransactionListener()
	
	// 创建事务检查器
	checker := transaction.NewDefaultTransactionChecker()
	
	// 创建半消息
	txID := transaction.TransactionID("test-tx-001")
	halfMsg := transaction.HalfMessage{
		TransactionID:   txID,
		Topic:          "test-topic",
		Partition:      0,
		Key:            []byte("test-key"),
		Value:          []byte("test-value"),
		Headers:        map[string]string{"test": "header"},
		ProducerGroup:  "test-group",
		CallbackAddress: fmt.Sprintf("localhost:%s", port),
	}
	
	// 先执行本地事务
	localState := listener.ExecuteLocalTransaction(txID, string(txID))
	assert.Equal(t, transaction.StateCommit, localState)
	
	// 通过TCP回调检查事务状态
	callbackState := checker.CheckTransactionStateNet(
		fmt.Sprintf("localhost:%s", port),
		txID,
		halfMsg,
	)
	
	// 验证回调结果
	assert.Equal(t, transaction.StateCommit, callbackState)
	assert.Equal(t, 1, listener.GetCallbackCount())
}

// TestTCPCallbackRollback 测试TCP回调回滚场景
func TestTCPCallbackRollback(t *testing.T) {
	c := &client.Client{}
	listener := NewSimpleMockTransactionListener()
	producer := client.NewTransactionProducer(c, listener, "test-producer-rollback")
	
	port := "18081"
	err := producer.StartTransactionListener(port)
	assert.NoError(t, err)
	
	time.Sleep(100 * time.Millisecond)
	defer producer.StopTransactionListener()
	
	checker := transaction.NewDefaultTransactionChecker()
	
	// 使用特殊的事务ID触发回滚
	txID := transaction.TransactionID("rollback-tx")
	halfMsg := transaction.HalfMessage{
		TransactionID:   txID,
		Topic:          "test-topic",
		Partition:      0,
		Key:            []byte("rollback-key"),
		Value:          []byte("rollback-value"),
		CallbackAddress: fmt.Sprintf("localhost:%s", port),
	}
	
	// 执行本地事务（应该返回回滚）
	localState := listener.ExecuteLocalTransaction(txID, string(txID))
	assert.Equal(t, transaction.StateRollback, localState)
	
	// TCP回调检查
	callbackState := checker.CheckTransactionStateNet(
		fmt.Sprintf("localhost:%s", port),
		txID,
		halfMsg,
	)
	
	assert.Equal(t, transaction.StateRollback, callbackState)
	assert.Equal(t, 1, listener.GetCallbackCount())
}

// TestTCPCallbackUnknownTransaction 测试未知事务的TCP回调
func TestTCPCallbackUnknownTransaction(t *testing.T) {
	c := &client.Client{}
	listener := NewSimpleMockTransactionListener()
	producer := client.NewTransactionProducer(c, listener, "test-producer-unknown")
	
	port := "18082"
	err := producer.StartTransactionListener(port)
	assert.NoError(t, err)
	
	time.Sleep(100 * time.Millisecond)
	defer producer.StopTransactionListener()
	
	checker := transaction.NewDefaultTransactionChecker()
	
	// 直接查询未执行的事务
	txID := transaction.TransactionID("non-existent-tx")
	halfMsg := transaction.HalfMessage{
		TransactionID:   txID,
		Topic:          "test-topic",
		Partition:      0,
		CallbackAddress: fmt.Sprintf("localhost:%s", port),
	}
	
	// 不执行本地事务，直接进行TCP回调
	callbackState := checker.CheckTransactionStateNet(
		fmt.Sprintf("localhost:%s", port),
		txID,
		halfMsg,
	)
	
	assert.Equal(t, transaction.StateUnknown, callbackState)
	assert.Equal(t, 1, listener.GetCallbackCount())
}

// TestTCPCallbackConcurrency 测试并发TCP回调
func TestTCPCallbackConcurrency(t *testing.T) {
	c := &client.Client{}
	listener := NewSimpleMockTransactionListener()
	producer := client.NewTransactionProducer(c, listener, "test-producer-concurrent")
	
	port := "18083"
	err := producer.StartTransactionListener(port)
	assert.NoError(t, err)
	
	time.Sleep(100 * time.Millisecond)
	defer producer.StopTransactionListener()
	
	checker := transaction.NewDefaultTransactionChecker()
	
	// 并发测试
	concurrency := 10
	var wg sync.WaitGroup
	results := make([]transaction.TransactionState, concurrency)
	
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			
			txID := transaction.TransactionID(fmt.Sprintf("concurrent-tx-%03d", index))
			halfMsg := transaction.HalfMessage{
				TransactionID:   txID,
				Topic:          "test-topic",
				Partition:      int32(index % 3),
				Key:            []byte(fmt.Sprintf("key-%d", index)),
				Value:          []byte(fmt.Sprintf("value-%d", index)),
				CallbackAddress: fmt.Sprintf("localhost:%s", port),
			}
			
			// 先执行本地事务
			listener.ExecuteLocalTransaction(txID, string(txID))
			
			// TCP回调检查
			state := checker.CheckTransactionStateNet(
				fmt.Sprintf("localhost:%s", port),
				txID,
				halfMsg,
			)
			
			results[index] = state
		}(i)
	}
	
	wg.Wait()
	
	// 验证所有回调都成功
	for i, state := range results {
		assert.Equal(t, transaction.StateCommit, state, "Transaction %d should be committed", i)
	}
	
	// 验证回调次数
	assert.Equal(t, concurrency, listener.GetCallbackCount())
}

// TestTCPCallbackTimeout 测试TCP回调超时
func TestTCPCallbackTimeout(t *testing.T) {
	checker := transaction.NewDefaultTransactionChecker()
	
	// 使用不存在的端口测试超时
	txID := transaction.TransactionID("timeout-tx")
	halfMsg := transaction.HalfMessage{
		TransactionID:   txID,
		Topic:          "test-topic",
		Partition:      0,
		CallbackAddress: "localhost:19999", // 不存在的端口
	}
	
	// 这应该超时并返回未知状态
	state := checker.CheckTransactionStateNet(
		"localhost:19999",
		txID,
		halfMsg,
	)
	
	// 连接失败时应该返回未知状态
	assert.Equal(t, transaction.StateUnknown, state)
}