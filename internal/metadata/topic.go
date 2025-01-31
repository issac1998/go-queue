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
	Leader   string   
	Replicas []string 
	Isr      []string 
	Mu       sync.RWMutex
}

// GetPartition defines
func GetPartition(string, int32) *Partition {
	return &Partition{}
}
