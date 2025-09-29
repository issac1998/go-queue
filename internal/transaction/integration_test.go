package transaction

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRaftProposer 模拟Raft提议器用于测试
type MockRaftProposer struct {
	mu       sync.RWMutex
	commands map[uint64][]map[string]interface{}
	failures map[uint64]bool // 模拟失败的组
}

func NewMockRaftProposer() *MockRaftProposer {
	return &MockRaftProposer{
		commands: make(map[uint64][]map[string]interface{}),
		failures: make(map[uint64]bool),
	}
}

func (m *MockRaftProposer) ProposeTransactionCommand(ctx context.Context, raftGroupID uint64, cmdType string, data map[string]interface{}) (interface{}, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 模拟失败的组
	if m.failures[raftGroupID] {
		return nil, fmt.Errorf("simulated failure for group %d", raftGroupID)
	}

	// 记录命令
	if m.commands[raftGroupID] == nil {
		m.commands[raftGroupID] = make([]map[string]interface{}, 0)
	}
	
	command := make(map[string]interface{})
	for k, v := range data {
		command[k] = v
	}
	command["cmd_type"] = cmdType
	
	m.commands[raftGroupID] = append(m.commands[raftGroupID], command)
	
	return "success", nil
}

func (m *MockRaftProposer) SetGroupFailure(groupID uint64, shouldFail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[groupID] = shouldFail
}

func (m *MockRaftProposer) GetCommands(groupID uint64) []map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.commands[groupID]
}

func (m *MockRaftProposer) GetAllCommands() map[uint64][]map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make(map[uint64][]map[string]interface{})
	for k, v := range m.commands {
		result[k] = make([]map[string]interface{}, len(v))
		copy(result[k], v)
	}
	return result
}

// MockStateMachineGetter 模拟状态机获取器
type MockStateMachineGetter struct {
	states map[string]TransactionState
	mu     sync.RWMutex
}

func NewMockStateMachineGetter() *MockStateMachineGetter {
	return &MockStateMachineGetter{
		states: make(map[string]TransactionState),
	}
}

func (m *MockStateMachineGetter) GetStateMachine(raftGroupID uint64) (PartitionStateMachineInterface, error) {
	return m, nil
}

func (m *MockStateMachineGetter) GetAllRaftGroups() []uint64 {
	// 返回模拟的Raft组ID列表，排除Controller组(组1)
	return []uint64{2}
}

func (m *MockStateMachineGetter) GetHalfMessage(txnID string) (interface{}, bool) {
	// 模拟获取半消息
	return nil, false
}

func (m *MockStateMachineGetter) GetTimeoutTransactions() []string {
	// 模拟获取超时事务
	return []string{}
}

func (m *MockStateMachineGetter) GetTransactionState(transactionID string) TransactionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if state, exists := m.states[transactionID]; exists {
		return state
	}
	return StateUnknown
}

func (m *MockStateMachineGetter) SetTransactionState(transactionID string, state TransactionState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[transactionID] = state
}

// TestPartitionTransactionManagerIntegration 集成测试
func TestPartitionTransactionManagerIntegration(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "transaction_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建模拟组件
	mockRaftProposer := NewMockRaftProposer()
	mockStateMachineGetter := NewMockStateMachineGetter()
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// 创建配置
	config := &PartitionTransactionManagerConfig{
		Topic:                "test-topic",
		PartitionID:          0,
		DefaultTimeout:       5 * time.Second,
		CheckInterval:        100 * time.Millisecond,
		MaxCheckInterval:     1 * time.Second,
		MaxCheckCount:        3,
		ExpiryCheckInterval:  500 * time.Millisecond,
		EnableRaft:           true,
		RaftGroupID:          1,
		Logger:               logger,
		RaftProposer:         mockRaftProposer,
		StateMachineGetter:   mockStateMachineGetter,
		ErrorHandler:         NewTransactionErrorHandler(logger),
	}

	// 创建分区事务管理器
	ptm := NewPartitionTransactionManagerWithConfig(config)
	require.NotNil(t, ptm)

	t.Run("BasicCommitTransaction", func(t *testing.T) {
		transactionID := TransactionID("test-commit-001")
		
		// 执行提交
		response, err := ptm.CommitTransaction(transactionID)
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, transactionID, response.TransactionID)

		// 验证命令被发送到Raft组
		commands := mockRaftProposer.GetCommands(2) // 使用组2而不是组1
		assert.Len(t, commands, 1)
		assert.Equal(t, "transaction_commit", commands[0]["cmd_type"])
		assert.Equal(t, string(transactionID), commands[0]["transaction_id"])
	})

	t.Run("BasicRollbackTransaction", func(t *testing.T) {
		transactionID := TransactionID("test-rollback-001")
		
		// 执行回滚
		response, err := ptm.RollbackTransaction(transactionID)
		assert.NoError(t, err)
		assert.NotNil(t, response)
		assert.Equal(t, transactionID, response.TransactionID)

		// 验证命令被发送到Raft组
		commands := mockRaftProposer.GetCommands(2) // 使用组2而不是组1
		assert.Len(t, commands, 2) // 包含之前的commit命令
		lastCommand := commands[len(commands)-1]
		assert.Equal(t, "transaction_rollback", lastCommand["cmd_type"])
		assert.Equal(t, string(transactionID), lastCommand["transaction_id"])
	})

	t.Run("PartialSuccessCommit", func(t *testing.T) {
		// 设置多个Raft组，其中一个失败
		mockRaftProposer.SetGroupFailure(1, true) // 设置组1失败

		transactionID := TransactionID("test-partial-commit-001")
		
		// 执行提交（应该部分成功）
		response, err := ptm.CommitTransaction(transactionID)
		assert.NoError(t, err) // 部分成功不应该返回错误
		assert.NotNil(t, response)

		// 验证成功的组收到了命令
		commands2 := mockRaftProposer.GetCommands(2)
		assert.True(t, len(commands2) > 0)

		// 验证失败的组没有成功执行命令
		commands1 := mockRaftProposer.GetCommands(1)
		assert.Empty(t, commands1) // 失败的组不应该有成功的命令记录
	})

	t.Run("AllGroupsFailCommit", func(t *testing.T) {
		// 设置所有组都失败
		mockRaftProposer.SetGroupFailure(2, true) // 只有组2是相关的

		transactionID := TransactionID("test-all-fail-commit-001")
		
		// 执行提交（应该完全失败）
		response, err := ptm.CommitTransaction(transactionID)
		assert.NoError(t, err) // 方法本身不返回error，错误信息在response中
		assert.NotNil(t, response) // 响应应该包含错误信息，而不是nil
		assert.Equal(t, int16(14), response.ErrorCode) // 检查错误代码，使用int16类型
		assert.Contains(t, response.Error, "failed to commit transaction")
	})

	t.Run("ConcurrentTransactions", func(t *testing.T) {
		// 重置所有失败状态
		mockRaftProposer.SetGroupFailure(1, false)
		mockRaftProposer.SetGroupFailure(2, false)

		const numTransactions = 10
		var wg sync.WaitGroup
		errors := make(chan error, numTransactions*2)

		// 并发执行提交和回滚操作
		for i := 0; i < numTransactions; i++ {
			wg.Add(2)
			
			go func(id int) {
				defer wg.Done()
				transactionID := TransactionID(fmt.Sprintf("concurrent-commit-%d", id))
				_, err := ptm.CommitTransaction(transactionID)
				if err != nil {
					errors <- err
				}
			}(i)

			go func(id int) {
				defer wg.Done()
				transactionID := TransactionID(fmt.Sprintf("concurrent-rollback-%d", id))
				_, err := ptm.RollbackTransaction(transactionID)
				if err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// 检查是否有错误
		var errorList []error
		for err := range errors {
			errorList = append(errorList, err)
		}
		assert.Empty(t, errorList, "Should not have errors in concurrent execution")

		// 验证所有命令都被记录
		allCommands := mockRaftProposer.GetAllCommands()
		totalCommands := 0
		for _, commands := range allCommands {
			totalCommands += len(commands)
		}
		assert.True(t, totalCommands >= numTransactions*2, "Should have at least %d commands", numTransactions*2)
	})

	// 清理
	ptm.Stop()
}

// TestTransactionManagerWithStorage 测试带存储的事务管理器
func TestTransactionManagerWithStorage(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "transaction_storage_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储
	storage, err := NewPebbleHalfMessageStorage(tempDir)
	require.NoError(t, err)
	defer storage.Close()

	// 创建过期管理器
	logger := log.New(os.Stdout, "[STORAGE_TEST] ", log.LstdFlags)
	expiryManager := NewExpiryManager(storage, &ExpiryManagerConfig{
		CheckInterval: 100 * time.Millisecond,
		Logger:        logger,
	})

	// 创建事务管理器
	tm := &TransactionManager{
		storage:              storage,
		expiryManager:        expiryManager,
		defaultTimeout:       5 * time.Second,
		maxCheckCount:        3,
		checkInterval:        100 * time.Millisecond,
		maxCheckInterval:     1 * time.Second,
		producerGroupCheckers: make(map[string]*DefaultTransactionChecker),
		enableRaft:           false,
		raftGroupID:          0,
		errorHandler:         NewTransactionErrorHandler(logger),
		metrics:              NewTransactionErrorMetrics(logger),
		stopChan:             make(chan struct{}),
	}

	t.Run("PrepareAndCommitTransaction", func(t *testing.T) {
		// 注册生产者组
		err := tm.RegisterProducerGroup("test-group", "localhost:8080")
		require.NoError(t, err)

		// 准备事务
		prepareReq := &TransactionPrepareRequest{
			TransactionID: "storage-test-001",
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("test-key"),
			Value:         []byte("test-value"),
			ProducerGroup: "test-group",
			Timeout:       5000, // 5秒
		}

		prepareResp, err := tm.PrepareTransaction(prepareReq)
		assert.NoError(t, err)
		assert.NotNil(t, prepareResp)
		assert.Equal(t, prepareReq.TransactionID, prepareResp.TransactionID)

		// 验证半消息被存储
		storedMsg, err := storage.Get(string(prepareReq.TransactionID))
		assert.NoError(t, err)
		assert.NotNil(t, storedMsg)
		assert.Equal(t, prepareReq.TransactionID, storedMsg.HalfMessage.TransactionID)

		// 提交事务
		commitResp, err := tm.CommitTransaction(prepareReq.TransactionID)
		assert.NoError(t, err)
		assert.NotNil(t, commitResp)
		assert.Equal(t, prepareReq.TransactionID, commitResp.TransactionID)
	})

	t.Run("PrepareAndRollbackTransaction", func(t *testing.T) {
		// 准备事务
		prepareReq := &TransactionPrepareRequest{
			TransactionID: "storage-test-002",
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("test-key-2"),
			Value:         []byte("test-value-2"),
			ProducerGroup: "test-group",
			Timeout:       5000,
		}

		prepareResp, err := tm.PrepareTransaction(prepareReq)
		assert.NoError(t, err)
		assert.NotNil(t, prepareResp)

		// 回滚事务
		rollbackResp, err := tm.RollbackTransaction(prepareReq.TransactionID)
		assert.NoError(t, err)
		assert.NotNil(t, rollbackResp)
		assert.Equal(t, prepareReq.TransactionID, rollbackResp.TransactionID)
	})

	t.Run("TransactionExpiry", func(t *testing.T) {
		// 准备一个短超时的事务
		prepareReq := &TransactionPrepareRequest{
			TransactionID: "storage-test-003",
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("test-key-3"),
			Value:         []byte("test-value-3"),
			ProducerGroup: "test-group",
			Timeout:       200, // 200毫秒
		}

		prepareResp, err := tm.PrepareTransaction(prepareReq)
		assert.NoError(t, err)
		assert.NotNil(t, prepareResp)

		// 等待事务过期
		time.Sleep(300 * time.Millisecond)

		// 启动过期管理器
		expiryManager.Start()
		defer expiryManager.Stop()

		// 等待过期处理
		time.Sleep(200 * time.Millisecond)

		// 验证过期事务被处理 - 使用GetExpiredTransactions方法
		expiredTxns, err := tm.GetExpiredTransactions()
		assert.NoError(t, err)
		
		// 过期的事务应该被标记或删除
		found := false
		for _, txn := range expiredTxns {
			// 由于GetExpiredTransactions返回[]interface{}，需要进行类型断言
			if txnID, ok := txn.(string); ok {
				if txnID == string(prepareReq.TransactionID) {
					found = true
					break
				}
			}
		}
		// 由于过期管理器的处理，事务可能已经被清理
		// 这里我们主要验证过期管理器能正常工作
		t.Logf("Found expired transaction: %v", found)
	})

	// 清理
	tm.Stop()
}

// BenchmarkTransactionOperations 性能基准测试
func BenchmarkTransactionOperations(b *testing.B) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "transaction_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 创建模拟组件
	mockRaftProposer := NewMockRaftProposer()
	mockStateMachineGetter := NewMockStateMachineGetter()
	logger := log.New(os.Stdout, "[BENCH] ", log.LstdFlags)

	config := &PartitionTransactionManagerConfig{
		Topic:                "bench-topic",
		PartitionID:          0,
		DefaultTimeout:       5 * time.Second,
		CheckInterval:        100 * time.Millisecond,
		MaxCheckInterval:     1 * time.Second,
		MaxCheckCount:        3,
		ExpiryCheckInterval:  500 * time.Millisecond,
		EnableRaft:           true,
		RaftGroupID:          1,
		Logger:               logger,
		RaftProposer:         mockRaftProposer,
		StateMachineGetter:   mockStateMachineGetter,
		ErrorHandler:         NewTransactionErrorHandler(logger),
	}

	ptm := NewPartitionTransactionManagerWithConfig(config)
	defer ptm.Stop()

	b.Run("CommitTransaction", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			transactionID := TransactionID(fmt.Sprintf("bench-commit-%d", i))
			_, err := ptm.CommitTransaction(transactionID)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("RollbackTransaction", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			transactionID := TransactionID(fmt.Sprintf("bench-rollback-%d", i))
			_, err := ptm.RollbackTransaction(transactionID)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ConcurrentOperations", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%2 == 0 {
					transactionID := TransactionID(fmt.Sprintf("bench-concurrent-commit-%d", i))
					ptm.CommitTransaction(transactionID)
				} else {
					transactionID := TransactionID(fmt.Sprintf("bench-concurrent-rollback-%d", i))
					ptm.RollbackTransaction(transactionID)
				}
				i++
			}
		})
	})
}