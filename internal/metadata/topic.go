package metadata

import (
	"go-queue/internal/storage"
	"sync"
)

// Topic defines
type Topic struct {
	Name       string
	Partitions []*Partition
}

// Partition defines
type Partition struct {
	ID int
	// Segments or Segment??
	Segment  *storage.Segment
	Segments []*storage.Segment
	Leader   string   // Leader 节点地址
	Replicas []string // 副本节点列表
	Isr      []string // In-Sync Replicas
	Mu       sync.RWMutex
}

// GetPartition defines
func GetPartition(string, int32) *Partition {
	return &Partition{}
}
