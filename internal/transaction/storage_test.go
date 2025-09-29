package transaction

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPebbleHalfMessageStorage(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "pebble_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储实例
	storage, err := NewPebbleHalfMessageStorage(tempDir)
	require.NoError(t, err)
	defer storage.Close()

	t.Run("Store and Get", func(t *testing.T) {
		// 创建测试数据
		txnID := "test-txn-001"
		halfMessage := &HalfMessage{
			TransactionID: TransactionID(txnID),
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("test-key"),
			Value:         []byte("test-value"),
			ProducerGroup: "test-group",
			CreatedAt:     time.Now(),
			State:         StatePrepared,
		}
		expireTime := time.Now().Add(time.Hour)

		// 存储半消息
		err := storage.Store(txnID, halfMessage, expireTime)
		assert.NoError(t, err)

		// 获取半消息
		retrieved, err := storage.Get(txnID)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, halfMessage.TransactionID, retrieved.HalfMessage.TransactionID)
		assert.Equal(t, halfMessage.Topic, retrieved.HalfMessage.Topic)
		assert.Equal(t, halfMessage.ProducerGroup, retrieved.HalfMessage.ProducerGroup)
	})

	t.Run("Delete", func(t *testing.T) {
		txnID := "test-txn-002"
		halfMessage := &HalfMessage{
			TransactionID: TransactionID(txnID),
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("test-key"),
			Value:         []byte("test-value"),
			ProducerGroup: "test-group",
			CreatedAt:     time.Now(),
			State:         StatePrepared,
		}
		expireTime := time.Now().Add(time.Hour)

		// 存储半消息
		err := storage.Store(txnID, halfMessage, expireTime)
		assert.NoError(t, err)

		// 删除半消息
		err = storage.Delete(txnID)
		assert.NoError(t, err)

		// 验证已删除
		retrieved, err := storage.Get(txnID)
		assert.Error(t, err)
		assert.Nil(t, retrieved)
	})

	t.Run("GetExpiredMessages", func(t *testing.T) {
		// 创建已过期的消息
		expiredTxnID := "expired-txn-001"
		expiredMessage := &HalfMessage{
			TransactionID: TransactionID(expiredTxnID),
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("expired-key"),
			Value:         []byte("expired-value"),
			ProducerGroup: "test-group",
			CreatedAt:     time.Now(),
			State:         StatePrepared,
		}
		expiredTime := time.Now().Add(-time.Hour) // 已过期

		// 创建未过期的消息
		validTxnID := "valid-txn-001"
		validMessage := &HalfMessage{
			TransactionID: TransactionID(validTxnID),
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("valid-key"),
			Value:         []byte("valid-value"),
			ProducerGroup: "test-group",
			CreatedAt:     time.Now(),
			State:         StatePrepared,
		}
		validTime := time.Now().Add(time.Hour) // 未过期

		// 存储消息
		err := storage.Store(expiredTxnID, expiredMessage, expiredTime)
		assert.NoError(t, err)
		err = storage.Store(validTxnID, validMessage, validTime)
		assert.NoError(t, err)

		// 获取过期消息
		expiredMessages, err := storage.GetExpiredMessages(time.Now())
		assert.NoError(t, err)
		assert.Len(t, expiredMessages, 1)
		assert.Equal(t, expiredTxnID, expiredMessages[0].TransactionID)
	})

	t.Run("CleanupExpired", func(t *testing.T) {
		// 创建已过期的消息
		expiredTxnID := "cleanup-expired-001"
		expiredMessage := &HalfMessage{
			TransactionID: TransactionID(expiredTxnID),
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("expired-key"),
			Value:         []byte("expired-value"),
			ProducerGroup: "test-group",
			CreatedAt:     time.Now(),
			State:         StatePrepared,
		}
		expiredTime := time.Now().Add(-time.Hour)

		// 存储消息
		err := storage.Store(expiredTxnID, expiredMessage, expiredTime)
		assert.NoError(t, err)

		// 清理过期消息
		err = storage.CleanupExpired(time.Now())
		assert.NoError(t, err)

		// 验证消息已被删除
		retrieved, err := storage.Get(expiredTxnID)
		assert.Error(t, err)
		assert.Nil(t, retrieved)
	})
}

func TestExpiryManager(t *testing.T) {
	// 创建临时目录用于测试
	tempDir, err := os.MkdirTemp("", "expiry_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储实例
	storage, err := NewPebbleHalfMessageStorage(tempDir)
	require.NoError(t, err)
	defer storage.Close()

	t.Run("ExpiryManager Basic Functionality", func(t *testing.T) {
		config := &ExpiryManagerConfig{
			CheckInterval: 100 * time.Millisecond,
			Logger:        nil, // 使用默认logger
		}

		manager := NewExpiryManager(storage, config)
		
		// 启动过期管理器
		manager.Start()
		defer manager.Stop()

		// 创建已过期的消息
		expiredTxnID := "expiry-manager-test-001"
		expiredMessage := &HalfMessage{
			TransactionID: TransactionID(expiredTxnID),
			Topic:         "test-topic",
			Partition:     0,
			Key:           []byte("expired-key"),
			Value:         []byte("expired-value"),
			ProducerGroup: "test-group",
			CreatedAt:     time.Now(),
			State:         StatePrepared,
		}
		expiredTime := time.Now().Add(-time.Hour)

		// 存储消息
		err = storage.Store(expiredTxnID, expiredMessage, expiredTime)
		assert.NoError(t, err)

		// 等待过期管理器处理
		time.Sleep(200 * time.Millisecond)

		// 验证消息已被删除（由于没有回调函数，只能检查删除）
		retrieved, err := storage.Get(expiredTxnID)
		assert.Error(t, err)
		assert.Nil(t, retrieved)
	})
}