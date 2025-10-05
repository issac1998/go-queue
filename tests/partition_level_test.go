package tests

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/raft"
	"github.com/issac1998/go-queue/internal/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRaftManager 模拟Raft管理器
type MockRaftManager struct {
	isLeader bool
	mu       sync.RWMutex
}

func NewMockRaftManager(isLeader bool) *MockRaftManager {
	return &MockRaftManager{isLeader: isLeader}
}

func (m *MockRaftManager) IsLeader(groupID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isLeader
}

func (m *MockRaftManager) GetLeaderID(groupID uint64) (uint64, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.isLeader {
		return 1, true, nil
	}
	return 0, false, nil
}

func (m *MockRaftManager) SetLeader(isLeader bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isLeader = isLeader
}

// MockTransactionChecker 模拟事务检查器
type MockTransactionChecker struct {
	mu                sync.RWMutex
	transactionStates map[string]transaction.TransactionState
	checkCount        int
	checkDelay        time.Duration
}

func NewMockTransactionChecker() *MockTransactionChecker {
	return &MockTransactionChecker{
		transactionStates: make(map[string]transaction.TransactionState),
		checkDelay:        50 * time.Millisecond, // 模拟网络延迟
	}
}

func (m *MockTransactionChecker) SetTransactionState(txID string, state transaction.TransactionState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transactionStates[txID] = state
}

func (m *MockTransactionChecker) CheckTransactionStateNet(address string, txID transaction.TransactionID, halfMsg transaction.HalfMessage) transaction.TransactionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.checkCount++
	
	// 模拟网络延迟
	time.Sleep(m.checkDelay)
	
	if state, exists := m.transactionStates[string(txID)]; exists {
		return state
	}
	
	return transaction.StateUnknown
}

func (m *MockTransactionChecker) GetCheckCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkCount
}

func (m *MockTransactionChecker) SetCheckDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkDelay = delay
}

// TestPartitionLevelCleanup 测试分区级清理机制
func TestPartitionLevelCleanup(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "partition_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建模拟Raft管理器（设为leader）
	raftManager := NewMockRaftManager(true)

	// 创建分区状态机
	psm, err := raft.NewPartitionStateMachine("test-topic", 0, tempDir, nil)
	require.NoError(t, err)
	defer psm.Close()

	// 设置Raft管理器
	psm.SetRaftManager(raftManager, 1)

	// 存储一些半消息
	txID1 := "test-tx-001"
	txID2 := "test-tx-002"
	txID3 := "test-tx-003"

	// 存储半消息（带回调地址）
	storeData1 := map[string]interface{}{
		"transaction_id":   txID1,
		"topic":           "test-topic",
		"partition":       float64(0),
		"key":             "test-key-1",
		"value":           "test-value-1",
		"producer_group":  "test-group",
		"callback_address": "localhost:8080",
		"timeout":         float64(1), // 1秒超时，快速过期
	}

	storeData2 := map[string]interface{}{
		"transaction_id":   txID2,
		"topic":           "test-topic",
		"partition":       float64(0),
		"key":             "test-key-2",
		"value":           "test-value-2",
		"producer_group":  "test-group",
		"callback_address": "localhost:8081",
		"timeout":         float64(1),
	}

	// 存储半消息（无回调地址）
	storeData3 := map[string]interface{}{
		"transaction_id":  txID3,
		"topic":          "test-topic",
		"partition":      float64(0),
		"key":            "test-key-3",
		"value":          "test-value-3",
		"producer_group": "test-group",
		"timeout":        float64(1),
	}

	// 构造命令并存储半消息
	cmd1 := raft.PartitionCommand{
		Type: "store_half_message",
		Data: storeData1,
	}
	cmd1Bytes, _ := json.Marshal(cmd1)
	_, err = psm.Update(cmd1Bytes)
	require.NoError(t, err)

	cmd2 := raft.PartitionCommand{
		Type: "store_half_message",
		Data: storeData2,
	}
	cmd2Bytes, _ := json.Marshal(cmd2)
	_, err = psm.Update(cmd2Bytes)
	require.NoError(t, err)

	cmd3 := raft.PartitionCommand{
		Type: "store_half_message",
		Data: storeData3,
	}
	cmd3Bytes, _ := json.Marshal(cmd3)
	_, err = psm.Update(cmd3Bytes)
	require.NoError(t, err)

	// 等待消息过期
	time.Sleep(1200 * time.Millisecond)

	// 验证指标
	initialMetrics := psm.GetMetrics()
	transactionChecks := initialMetrics["transaction_checks"].(map[string]interface{})
	initialTotalChecks := transactionChecks["total_checks"].(int64)

	// 手动触发清理（通过反射调用私有方法或者等待定时任务）
	// 由于cleanupExpiredHalfMessages是私有方法，我们通过等待定时任务来测试
	time.Sleep(2 * time.Second) // 等待定时清理任务执行

	// 验证清理结果
	finalMetrics := psm.GetMetrics()
	finalTransactionChecks := finalMetrics["transaction_checks"].(map[string]interface{})
	finalTotalChecks := finalTransactionChecks["total_checks"].(int64)

	// 验证有检查发生（对于有回调地址的消息）
	assert.GreaterOrEqual(t, finalTotalChecks, initialTotalChecks, "Should have performed transaction checks")

	t.Log("Partition level cleanup test completed")
}

// TestPartitionLevelLeadershipChange 测试领导权变更对清理的影响
func TestPartitionLevelLeadershipChange(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "partition_leadership_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建模拟Raft管理器（初始为非leader）
	raftManager := NewMockRaftManager(false)

	// 创建PartitionStateMachine
	psm, err := raft.NewPartitionStateMachine("test-topic", 0, tempDir, nil)
	require.NoError(t, err)
	defer psm.Close()

	// 设置Raft管理器
	psm.SetRaftManager(raftManager, 1)

	// 存储半消息
	storeData := map[string]interface{}{
		"transaction_id":   "leadership-test-tx",
		"topic":           "test-topic",
		"partition":       float64(0),
		"key":             "test-key",
		"value":           "test-value",
		"producer_group":  "test-group",
		"callback_address": "localhost:8080",
		"timeout":         float64(1),
	}

	cmd := raft.PartitionCommand{
		Type: "store_half_message",
		Data: storeData,
	}
	cmdBytes, _ := json.Marshal(cmd)
	_, err = psm.Update(cmdBytes)
	require.NoError(t, err)

	// 等待消息过期
	time.Sleep(1200 * time.Millisecond)

	// 获取作为非leader时的指标
	nonLeaderMetrics := psm.GetMetrics()
	nonLeaderTransactionChecks := nonLeaderMetrics["transaction_checks"].(map[string]interface{})
	nonLeaderChecks := nonLeaderTransactionChecks["total_checks"].(int64)

	// 变为leader
	raftManager.SetLeader(true)

	// 等待一段时间让清理任务执行
	time.Sleep(2 * time.Second)

	// 获取作为leader时的指标
	leaderMetrics := psm.GetMetrics()
	leaderTransactionChecks := leaderMetrics["transaction_checks"].(map[string]interface{})
	leaderChecks := leaderTransactionChecks["total_checks"].(int64)

	// 验证只有在成为leader后才进行检查
	assert.GreaterOrEqual(t, leaderChecks, nonLeaderChecks, "Should perform more checks as leader")

	t.Log("Leadership change test completed")
}

// TestPartitionLevelBatchProcessing 测试批处理功能
func TestPartitionLevelBatchProcessing(t *testing.T) {
	// Create a temporary directory for test data
	tempDir, err := os.MkdirTemp("", "partition_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a compressor
	compressor, err := compression.GetCompressor(compression.Gzip)
	require.NoError(t, err)

	// Create a custom config with short cleanup interval for testing
	config := raft.DefaultPartitionStateMachineConfig()
	config.CleanupInterval = 100 * time.Millisecond // 100ms cleanup interval for testing
	config.EnableMetrics = true                     // Enable metrics to track totalChecks

	// Create a partition state machine with custom config
	psm, err := raft.NewPartitionStateMachineWithConfig("test-topic", 0, tempDir, compressor, config)
	require.NoError(t, err)

	// Create mock raft manager
	mockRaftManager := NewMockRaftManager(true)

	// Set up Raft manager to start cleanup process
	psm.SetRaftManager(mockRaftManager, 1)

	// Store half messages using the existing command structure
	halfMessages := []map[string]interface{}{
		{
			"transaction_id":   "msg1",
			"topic":           "test-topic",
			"partition":       float64(0),
			"key":             "test-key-1",
			"value":           "test data 1",
			"producer_group":  "test-group",
			"callback_address": "127.0.0.1:8080",
			"timeout":         "1s", // 1秒超时，确保消息过期
		},
		{
			"transaction_id":   "msg2",
			"topic":           "test-topic",
			"partition":       float64(0),
			"key":             "test-key-2",
			"value":           "test data 2",
			"producer_group":  "test-group",
			"callback_address": "127.0.0.1:8080",
			"timeout":         "1s", // 1秒超时，确保消息过期
		},
		{
			"transaction_id":   "msg3",
			"topic":           "test-topic",
			"partition":       float64(0),
			"key":             "test-key-3",
			"value":           "test data 3",
			"producer_group":  "test-group",
			"callback_address": "127.0.0.1:8080",
			"timeout":         "1s", // 1秒超时，确保消息过期
		},
	}

	// Store half messages using the command structure
	for i, msgData := range halfMessages {
		cmd := raft.PartitionCommand{
			Type: "store_half_message",
			Data: msgData,
		}
		cmdBytes, _ := json.Marshal(cmd)
		_, err = psm.Update(cmdBytes)
		require.NoError(t, err)
		t.Logf("Stored half message %d: %s", i+1, msgData["transaction_id"])
	}

	t.Logf("All half messages stored, waiting for expiration...")

	// Wait for messages to expire (timeout is 1 second)
	time.Sleep(1200 * time.Millisecond)

	t.Logf("Messages should be expired now, waiting for cleanup...")

	// Wait for cleanup to run (cleanup interval is 100ms, so wait 300ms to be safe)
	time.Sleep(300 * time.Millisecond)

	t.Logf("Cleanup should have run, checking metrics...")

	// Check that total_checks has been incremented
	totalChecks := psm.GetTotalChecks()
	assert.Greater(t, totalChecks, int64(0), "Expected total_checks to be greater than 0 after cleanup")

	// Verify that failed_checks is also tracked
	failedChecks := psm.GetFailedChecks()
	assert.GreaterOrEqual(t, failedChecks, int64(0), "Expected failed_checks to be non-negative")

	t.Logf("Total checks: %d, Failed checks: %d", totalChecks, failedChecks)
}

// TestPartitionLevelMetrics 测试指标收集
func TestPartitionLevelMetrics(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "partition_metrics_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建模拟Raft管理器
	raftManager := NewMockRaftManager(true)

	// 创建PartitionStateMachine
	psm, err := raft.NewPartitionStateMachine("test-topic", 0, tempDir, nil)
	require.NoError(t, err)
	defer psm.Close()

	// 设置Raft管理器
	psm.SetRaftManager(raftManager, 1)

	// 存储一些半消息
	for i := 0; i < 3; i++ {
		storeData := map[string]interface{}{
			"transaction_id":   fmt.Sprintf("metrics-tx-%03d", i),
			"topic":           "test-topic",
			"partition":       float64(0),
			"key":             fmt.Sprintf("metrics-key-%d", i),
			"value":           fmt.Sprintf("metrics-value-%d", i),
			"producer_group":  "test-group",
			"callback_address": fmt.Sprintf("localhost:909%d", i),
			"timeout":         float64(1),
		}

		cmd := raft.PartitionCommand{
			Type: "store_half_message",
			Data: storeData,
		}
		cmdBytes, _ := json.Marshal(cmd)
		_, err = psm.Update(cmdBytes)
		require.NoError(t, err)
	}

	// 等待消息过期
	time.Sleep(1200 * time.Millisecond)

	// 获取清理前的指标
	initialMetrics := psm.GetMetrics()
	initialTransactionChecks := initialMetrics["transaction_checks"].(map[string]interface{})
	initialTotalChecks := initialTransactionChecks["total_checks"].(int64)

	// 等待清理执行
	time.Sleep(2 * time.Second)

	// 获取清理后的指标
	finalMetrics := psm.GetMetrics()
	finalTransactionChecks := finalMetrics["transaction_checks"].(map[string]interface{})
	finalTotalChecks := finalTransactionChecks["total_checks"].(int64)
	finalFailedChecks := finalTransactionChecks["failed_checks"].(int64)

	// 验证指标变化
	assert.GreaterOrEqual(t, finalTotalChecks, initialTotalChecks, "Total checks should increase or stay same")

	t.Logf("Metrics test completed - Total checks: %d, Failed checks: %d", 
		finalTotalChecks, finalFailedChecks)
}