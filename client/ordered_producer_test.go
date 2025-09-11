package client

import (
	"testing"
)

func TestOrderedPartitioner(t *testing.T) {
	partitioner := NewOrderedPartitioner()

	messageGroup := "user-123"
	numPartitions := int32(4)

	partition1 := partitioner.SelectPartitionForMessageGroup(messageGroup, numPartitions)
	partition2 := partitioner.SelectPartitionForMessageGroup(messageGroup, numPartitions)
	partition3 := partitioner.SelectPartitionForMessageGroup(messageGroup, numPartitions)

	if partition1 != partition2 || partition2 != partition3 {
		t.Errorf("Same MessageGroup should route to same partition, got %d, %d, %d",
			partition1, partition2, partition3)
	}

	// 验证分区在有效范围内
	if partition1 < 0 || partition1 >= numPartitions {
		t.Errorf("Partition %d is out of range [0, %d)", partition1, numPartitions)
	}
}

func TestOrderedPartitionerDifferentGroups(t *testing.T) {
	partitioner := NewOrderedPartitioner()
	numPartitions := int32(4)

	
	groups := []string{"user-1", "user-2", "user-3", "user-4", "user-5"}
	partitions := make([]int32, len(groups))

	for i, group := range groups {
		partitions[i] = partitioner.SelectPartitionForMessageGroup(group, numPartitions)

		if partitions[i] < 0 || partitions[i] >= numPartitions {
			t.Errorf("Partition %d for group %s is out of range [0, %d)",
				partitions[i], group, numPartitions)
		}
	}

	partitionCount := make(map[int32]int)
	for _, partition := range partitions {
		partitionCount[partition]++
	}

	t.Logf("Partition distribution: %v", partitionCount)

	if len(partitionCount) < 2 {
		t.Logf("Warning: All groups mapped to same partition, may indicate poor distribution")
	}
}

func TestOrderedPartitionerHashConsistency(t *testing.T) {
	partitioner1 := NewOrderedPartitioner()
	partitioner2 := NewOrderedPartitioner()

	messageGroup := "test-group"
	numPartitions := int32(3)

	// 不同的分区器实例对相同输入应该产生相同结果
	partition1 := partitioner1.SelectPartitionForMessageGroup(messageGroup, numPartitions)
	partition2 := partitioner2.SelectPartitionForMessageGroup(messageGroup, numPartitions)

	if partition1 != partition2 {
		t.Errorf("Different partitioner instances should produce same result for same input, got %d vs %d",
			partition1, partition2)
	}
}

func TestOrderedPartitionerEdgeCases(t *testing.T) {
	partitioner := NewOrderedPartitioner()

	partition := partitioner.SelectPartitionForMessageGroup("any-group", 1)
	if partition != 0 {
		t.Errorf("With single partition, result should be 0, got %d", partition)
	}

	partition = partitioner.SelectPartitionForMessageGroup("any-group", 0)
	if partition != 0 {
		t.Errorf("With zero partitions, result should be 0, got %d", partition)
	}

	partition = partitioner.SelectPartitionForMessageGroup("", 4)
	if partition < 0 || partition >= 4 {
		t.Errorf("Empty MessageGroup should still produce valid partition, got %d", partition)
	}
}

func TestNewOrderedProducer(t *testing.T) {
	client := NewClient(ClientConfig{})

	producer := NewOrderedProducer(client)

	if producer == nil {
		t.Fatal("NewOrderedProducer returned nil")
	}

	if producer.client != client {
		t.Error("OrderedProducer client not set correctly")
	}

	if producer.partitioner == nil {
		t.Error("OrderedProducer partitioner not initialized")
	}
}
