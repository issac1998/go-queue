package metadata

import "example.com/m/v2/go-queue/internal/storage"

type Topic struct {
	Name       string
	Partitions []*Partition
}

type Partition struct {
	ID       int
	Segments []*storage.Segment
	Leader   string   // Leader 节点地址
	Replicas []string // 副本节点列表
	Isr      []string // In-Sync Replicas
}
