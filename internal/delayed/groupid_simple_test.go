package delayed

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroupIDInDelayedMessage 测试DelayedMessage结构体中的GroupID字段
func TestGroupIDInDelayedMessage(t *testing.T) {
	now := time.Now().UnixMilli()
	
	// 创建带有GroupID的延迟消息
	message := &DelayedMessage{
		ID:          "test-msg-1",
		GroupID:     100,
		Topic:       "test-topic",
		Partition:   0,
		Key:         []byte("test-key"),
		Value:       []byte("test-value"),
		CreateTime:  now,
		DeliverTime: now + 60000,
		Status:      StatusPending,
		RetryCount:  0,
		MaxRetries:  3,
		UpdateTime:  now,
	}

	// 验证GroupID字段
	assert.Equal(t, uint64(100), message.GroupID, "GroupID should be set correctly")

	// 测试JSON序列化和反序列化
	jsonData, err := json.Marshal(message)
	require.NoError(t, err)

	var deserializedMessage DelayedMessage
	err = json.Unmarshal(jsonData, &deserializedMessage)
	require.NoError(t, err)

	// 验证反序列化后GroupID保持不变
	assert.Equal(t, uint64(100), deserializedMessage.GroupID, "GroupID should be preserved after JSON serialization")
}

// TestDelayedMessageManagerScheduleWithRaftGroup 测试ScheduleMessageWithRaftGroup方法
func TestDelayedMessageManagerScheduleWithRaftGroup(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "delayed_manager_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储
	storage, err := NewPebbleDelayedMessageStorage(tempDir)
	require.NoError(t, err)
	defer storage.Close()

	// 创建管理器
	config := DelayedMessageManagerConfig{
		Storage: storage,
	}
	manager := NewDelayedMessageManager(config, nil)

	// 测试请求
	req := &DelayedProduceRequest{
		Topic:     "test-topic",
		Partition: 0,
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		DelayTime: 60000, // 1分钟延迟
	}

	raftGroupID := uint64(200)

	// 调用ScheduleMessageWithRaftGroup
	response, cmdData, err := manager.ScheduleMessageWithRaftGroup(req, raftGroupID)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.NotNil(t, cmdData)

	// 验证响应
	assert.NotEmpty(t, response.MessageID, "MessageID should not be empty")
	assert.Greater(t, response.DeliverTime, int64(0), "DeliverTime should be positive")

	// 验证命令数据
	var cmd map[string]interface{}
	err = json.Unmarshal(cmdData, &cmd)
	require.NoError(t, err)

	assert.Equal(t, "store_delayed_message", cmd["type"], "Command type should be store_delayed_message")

	data, ok := cmd["data"].(map[string]interface{})
	require.True(t, ok, "Command should have data field")

	messageID, ok := data["message_id"].(string)
	require.True(t, ok, "Data should have message_id field")
	assert.Equal(t, response.MessageID, messageID, "MessageID should match")

	// 验证消息中的GroupID
	messageData, ok := data["message"].(map[string]interface{})
	require.True(t, ok, "Data should have message field")

	groupID, ok := messageData["group_id"].(float64) // JSON数字解析为float64
	require.True(t, ok, "Message should have group_id field")
	assert.Equal(t, float64(raftGroupID), groupID, "GroupID should match raftGroupID")

	t.Logf("Successfully created delayed message with GroupID: %d", uint64(groupID))
}

// TestStorageGroupIDIsolation 测试存储层的GroupID隔离功能
func TestStorageGroupIDIsolation(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "storage_groupid_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// 创建存储
	storage, err := NewPebbleDelayedMessageStorage(tempDir)
	require.NoError(t, err)
	defer storage.Close()

	now := time.Now().UnixMilli()
	deliverTime := now + 60000

	// 创建不同GroupID的消息
	messages := []*DelayedMessage{
		{
			ID:          "msg1",
			GroupID:     1,
			Topic:       "test-topic",
			Partition:   0,
			Value:       []byte("value1"),
			CreateTime:  now,
			DeliverTime: deliverTime,
			Status:      StatusPending,
		},
		{
			ID:          "msg2",
			GroupID:     1,
			Topic:       "test-topic",
			Partition:   0,
			Value:       []byte("value2"),
			CreateTime:  now,
			DeliverTime: deliverTime + 1000,
			Status:      StatusPending,
		},
		{
			ID:          "msg3",
			GroupID:     2,
			Topic:       "test-topic",
			Partition:   1,
			Value:       []byte("value3"),
			CreateTime:  now,
			DeliverTime: deliverTime + 2000,
			Status:      StatusPending,
		},
	}

	// 存储所有消息
	for _, msg := range messages {
		err := storage.Store(msg.ID, msg)
		require.NoError(t, err, "Failed to store message %s", msg.ID)
	}

	// 测试GroupID=1的消息扫描
	fromTime := time.Unix(0, 0)
	toTime := time.Unix((deliverTime+10000)/1000, 0)

	group1Messages, err := storage.ScanPendingMessages(1, fromTime, toTime, 10)
	require.NoError(t, err)

	// 应该只返回GroupID=1的消息
	assert.Len(t, group1Messages, 2, "Should return 2 messages for group 1")

	for _, msg := range group1Messages {
		assert.Equal(t, uint64(1), msg.GroupID, "All messages should belong to group 1")
		assert.Contains(t, []string{"msg1", "msg2"}, msg.ID, "Should contain msg1 or msg2")
	}

	// 测试GroupID=2的消息扫描
	group2Messages, err := storage.ScanPendingMessages(2, fromTime, toTime, 10)
	require.NoError(t, err)

	// 应该只返回GroupID=2的消息
	assert.Len(t, group2Messages, 1, "Should return 1 message for group 2")
	assert.Equal(t, uint64(2), group2Messages[0].GroupID, "Message should belong to group 2")
	assert.Equal(t, "msg3", group2Messages[0].ID, "Should be msg3")

	// 测试不存在的GroupID
	group3Messages, err := storage.ScanPendingMessages(3, fromTime, toTime, 10)
	require.NoError(t, err)
	assert.Len(t, group3Messages, 0, "Should return no messages for non-existent group")

	t.Logf("GroupID isolation test passed: Group1=%d messages, Group2=%d messages, Group3=%d messages",
		len(group1Messages), len(group2Messages), len(group3Messages))
}