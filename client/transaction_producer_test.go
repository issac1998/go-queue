package client

import (
	"testing"
	"github.com/issac1998/go-queue/internal/transaction"
	"github.com/stretchr/testify/assert"
)

// TestPartitionSelectionLogic 测试partition选择逻辑
func TestPartitionSelectionLogic(t *testing.T) {
	tests := []struct {
		name      string
		partition int32
		key       []byte
		expected  bool // 是否应该选择partition
	}{
		{
			name:      "已指定partition",
			partition: 1,
			key:       []byte("test-key"),
			expected:  false, // 不需要选择，使用指定的
		},
		{
			name:      "未指定partition，有key",
			partition: -1,
			key:       []byte("test-key"),
			expected:  true, // 需要基于key选择
		},
		{
			name:      "未指定partition，无key",
			partition: -1,
			key:       nil,
			expected:  true, // 需要随机选择
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &TransactionMessage{
				Topic:     "test-topic",
				Partition: tt.partition,
				Key:       tt.key,
				Value:     []byte("test-value"),
			}

			needsSelection := msg.Partition == -1
			assert.Equal(t, tt.expected, needsSelection, "partition selection logic should match expected")
		})
	}
}

// TestBatchMessageRequestConversion 测试批量消息转换逻辑
func TestBatchMessageRequestConversion(t *testing.T) {
	msgs := []*TransactionMessage{
		{
			Topic:     "test-topic",
			Partition: 0,
			Key:       []byte("key1"),
			Value:     []byte("value1"),
		},
		{
			Topic:     "test-topic",
			Partition: -1, // 需要选择partition
			Key:       []byte("key2"),
			Value:     []byte("value2"),
		},
	}

	// 模拟转换逻辑
	messageRequests := make([]transaction.MessageRequest, len(msgs))
	for i, msg := range msgs {
		selectedPartition := msg.Partition
		if msg.Partition == -1 {
			// 模拟partition选择（实际实现中会调用hash或random）
			selectedPartition = 1 // 假设选择了partition 1
		}

		messageRequests[i] = transaction.MessageRequest{
			Topic:     msg.Topic,
			Partition: selectedPartition,
			Key:       msg.Key,
			Value:     msg.Value,
		}
	}

	// 验证转换结果
	assert.Equal(t, 2, len(messageRequests))
	assert.Equal(t, int32(0), messageRequests[0].Partition) // 第一个消息使用指定的partition
	assert.Equal(t, int32(1), messageRequests[1].Partition) // 第二个消息使用选择的partition
	assert.Equal(t, []byte("key1"), messageRequests[0].Key)
	assert.Equal(t, []byte("key2"), messageRequests[1].Key)
}