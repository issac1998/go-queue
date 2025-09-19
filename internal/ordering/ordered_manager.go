package ordering

import (
	"fmt"
	"sync"
	"time"
)

// OrderedMessageManager manages ordered message processing for all partitions
type OrderedMessageManager struct {
	partitionBuffers map[string]*ProducerBuffer
	windowSize       int
	timeoutDuration  time.Duration
	mu               sync.RWMutex

	stopCleanup chan struct{}
	cleanupDone chan struct{}
}

// NewOrderedMessageManager creates a new ordered message manager
func NewOrderedMessageManager(windowSize int, timeoutDuration time.Duration) *OrderedMessageManager {
	omm := &OrderedMessageManager{
		partitionBuffers: make(map[string]*ProducerBuffer),
		windowSize:       windowSize,
		timeoutDuration:  timeoutDuration,
		stopCleanup:      make(chan struct{}),
		cleanupDone:      make(chan struct{}),
	}

	go omm.cleanupRoutine()

	return omm
}

// ProcessMessage processes a message for ordering
func (omm *OrderedMessageManager) ProcessMessage(partition int32, producerID string, seqNum int64, data interface{}) ([]*PendingMessage, error) {
	key := fmt.Sprintf("%d:%s", partition, producerID)

	omm.mu.Lock()
	buffer, exists := omm.partitionBuffers[key]
	if !exists {
		buffer = NewProducerBuffer(producerID, omm.windowSize, omm.timeoutDuration)
		omm.partitionBuffers[key] = buffer
	}
	omm.mu.Unlock()

	return buffer.ProcessMessage(seqNum, data)
}

// GetProducerBuffer gets or creates a producer buffer for a partition
func (omm *OrderedMessageManager) GetProducerBuffer(partition int32, producerID string) *ProducerBuffer {
	key := fmt.Sprintf("%d:%s", partition, producerID)

	omm.mu.Lock()
	defer omm.mu.Unlock()

	buffer, exists := omm.partitionBuffers[key]
	if !exists {
		buffer = NewProducerBuffer(producerID, omm.windowSize, omm.timeoutDuration)
		omm.partitionBuffers[key] = buffer
	}

	return buffer
}

// ForceFlushProducer forces flush of all pending messages for a producer
func (omm *OrderedMessageManager) ForceFlushProducer(partition int32, producerID string) []*PendingMessage {
	key := fmt.Sprintf("%d:%s", partition, producerID)

	omm.mu.RLock()
	buffer, exists := omm.partitionBuffers[key]
	omm.mu.RUnlock()

	if !exists {
		return nil
	}

	return buffer.ForceFlush()
}

// GetTimeoutMessages returns all timeout messages across all producers
func (omm *OrderedMessageManager) GetTimeoutMessages() map[string][]*PendingMessage {
	omm.mu.RLock()
	defer omm.mu.RUnlock()

	timeoutMessages := make(map[string][]*PendingMessage)

	for key, buffer := range omm.partitionBuffers {
		timeouts := buffer.GetTimeoutMessages()
		if len(timeouts) > 0 {
			timeoutMessages[key] = timeouts
		}
	}

	return timeoutMessages
}

// cleanupRoutine periodically cleans up expired buffers and handles timeouts
func (omm *OrderedMessageManager) cleanupRoutine() {
	defer close(omm.cleanupDone)

	ticker := time.NewTicker(omm.timeoutDuration / 2)
	defer ticker.Stop()

	for {
		select {
		case <-omm.stopCleanup:
			return
		case <-ticker.C:
			omm.performCleanup()
		}
	}
}

func (omm *OrderedMessageManager) performCleanup() {
	now := time.Now()
	inactiveThreshold := omm.timeoutDuration * 5 

	omm.mu.Lock()
	defer omm.mu.Unlock()

	for key, buffer := range omm.partitionBuffers {
		if now.Sub(buffer.LastActivity) > inactiveThreshold {
			if buffer.PendingMessages.Len() > 0 {
				buffer.ForceFlush()
			}
			delete(omm.partitionBuffers, key)
		}
	}
}

// GetStats returns statistics for all buffers
func (omm *OrderedMessageManager) GetStats() map[string]interface{} {
	omm.mu.RLock()
	defer omm.mu.RUnlock()

	stats := map[string]interface{}{
		"total_buffers": len(omm.partitionBuffers),
		"window_size":   omm.windowSize,
		"timeout":       omm.timeoutDuration,
		"buffers":       make(map[string]interface{}),
	}

	bufferStats := stats["buffers"].(map[string]interface{})
	for key, buffer := range omm.partitionBuffers {
		bufferStats[key] = buffer.GetStats()
	}

	return stats
}

// Close stops the cleanup routine and releases resources
func (omm *OrderedMessageManager) Close() error {
	close(omm.stopCleanup)

	select {
	case <-omm.cleanupDone:
	case <-time.After(time.Second * 5):
	}

	omm.mu.Lock()
	defer omm.mu.Unlock()

	for _, buffer := range omm.partitionBuffers {
		buffer.ForceFlush()
	}

	return nil
}
