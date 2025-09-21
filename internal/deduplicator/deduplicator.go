package deduplicator

import (
	"sync"
	"time"
)

// Deduplicator maintains the state of a producer for deduplicator
type Deduplicator struct {
	ProducerID      string                   `json:"producer_id"`
	LastSequenceNum map[int32]int64          `json:"last_sequence_num"` 
	LastUpdateTime  time.Time                `json:"last_update_time"`
	CreatedTime     time.Time                `json:"created_time"`
	receivedSeqNums map[int32]map[int64]bool `json:"-"` 
	mu              sync.RWMutex             `json:"-"`
}

// NewDeduplicator creates a new Deduplicator
func NewDeduplicator(producerID string) *Deduplicator {
	now := time.Now()
	return &Deduplicator{
		ProducerID:      producerID,
		LastSequenceNum: make(map[int32]int64),
		LastUpdateTime:  now,
		CreatedTime:     now,
		receivedSeqNums: make(map[int32]map[int64]bool),
	}
}

// GetLastSequenceNumber returns the last sequence number for a partition
func (ps *Deduplicator) GetLastSequenceNumber(partition int32) int64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.LastSequenceNum[partition]
}

// UpdateSequenceNumber updates the sequence number for a partition
func (ps *Deduplicator) UpdateSequenceNumber(partition int32, seqNum int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.LastSequenceNum[partition] = seqNum
	ps.LastUpdateTime = time.Now()

	if ps.receivedSeqNums[partition] == nil {
		ps.receivedSeqNums[partition] = make(map[int64]bool)
	}
	ps.receivedSeqNums[partition][seqNum] = true

	const maxWindow = 1000
	for seq := range ps.receivedSeqNums[partition] {
		if seq < seqNum-maxWindow {
			delete(ps.receivedSeqNums[partition], seq)
		}
	}
}

// IsValidSequenceNumber checks if the sequence number is valid
// For AsyncIO mode, allows out-of-order messages within a reasonable window
// For sync mode, requires strict ordering (seqNum must be lastSeq + 1)
func (ps *Deduplicator) IsValidSequenceNumber(partition int32, seqNum int64, asyncIO bool) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	lastSeq := ps.LastSequenceNum[partition]

	if asyncIO {
		const maxWindow = 1000
		return seqNum > lastSeq-maxWindow && seqNum <= lastSeq+maxWindow
	} else {
		return seqNum == lastSeq+1
	}
}

// IsDuplicateSequenceNumber checks if the sequence number is a duplicate
func (ps *Deduplicator) IsDuplicateSequenceNumber(partition int32, seqNum int64, asyncIO bool) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	lastSeq := ps.LastSequenceNum[partition]

	if asyncIO {
		if ps.receivedSeqNums[partition] != nil {
			return ps.receivedSeqNums[partition][seqNum]
		}
		return false
	}
	return seqNum <= lastSeq

}

// DeduplicatorManager manages producer states for deduplicator
type DeduplicatorManager struct {
	states map[string]*Deduplicator
	mu     sync.RWMutex
}

// NewDeduplicatorManager creates a new producer state manager
func NewDeduplicatorManager() *DeduplicatorManager {
	return &DeduplicatorManager{
		states: make(map[string]*Deduplicator),
	}
}

// GetOrCreatededuplicator gets or creates a producer state
func (psm *DeduplicatorManager) GetOrCreatededuplicator(producerID string) *Deduplicator {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	state, exists := psm.states[producerID]
	if !exists {
		state = NewDeduplicator(producerID)
		psm.states[producerID] = state
	}
	return state
}

// GetNextSequenceNumber gets the next sequence number for a producer and partition
func (psm *DeduplicatorManager) GetNextSequenceNumber(producerID string, partition int32) int64 {
	state := psm.GetOrCreatededuplicator(producerID)
	lastSeq := state.GetLastSequenceNumber(partition)
	nextSeq := lastSeq + 1
	state.UpdateSequenceNumber(partition, nextSeq)
	return nextSeq
}

// Getdeduplicator gets a producer state
func (psm *DeduplicatorManager) Getdeduplicator(producerID string) (*Deduplicator, bool) {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	state, exists := psm.states[producerID]
	return state, exists
}

// Removededuplicator removes a producer state
func (psm *DeduplicatorManager) Removededuplicator(producerID string) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	delete(psm.states, producerID)
}

// GetAlldeduplicators returns all producer states
func (psm *DeduplicatorManager) GetAlldeduplicators() map[string]*Deduplicator {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	result := make(map[string]*Deduplicator)
	for id, state := range psm.states {
		result[id] = state
	}
	return result
}

// CleanupExpiredStates removes producer states that haven't been updated for a long time
func (psm *DeduplicatorManager) CleanupExpiredStates(expiration time.Duration) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	now := time.Now()
	for id, state := range psm.states {
		state.mu.RLock()
		lastUpdate := state.LastUpdateTime
		state.mu.RUnlock()

		if now.Sub(lastUpdate) > expiration {
			delete(psm.states, id)
		}
	}
}

// Setdeduplicator sets a producer state
func (psm *DeduplicatorManager) Setdeduplicator(producerID string, state *Deduplicator) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	psm.states[producerID] = state
}
