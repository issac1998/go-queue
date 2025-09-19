package ordering

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// PendingMessage represents a message waiting to be processed in order
type PendingMessage struct {
	SequenceNumber int64
	Data           interface{}
	Timestamp      time.Time
}

// MessageHeap implements heap.Interface for PendingMessage
type MessageHeap []*PendingMessage

func (h MessageHeap) Len() int           { return len(h) }
func (h MessageHeap) Less(i, j int) bool { return h[i].SequenceNumber < h[j].SequenceNumber }
func (h MessageHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MessageHeap) Push(x interface{}) {
	*h = append(*h, x.(*PendingMessage))
}

func (h *MessageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// ProducerBuffer manages ordered message processing for a single producer
type ProducerBuffer struct {
	ProducerID       string
	ExpectedSeq      int64
	PendingMessages  *MessageHeap
	WindowSize       int
	TimeoutDuration  time.Duration
	LastActivity     time.Time
	mu               sync.RWMutex
}

// NewProducerBuffer creates a new producer buffer
func NewProducerBuffer(producerID string, windowSize int, timeout time.Duration) *ProducerBuffer {
	msgHeap := &MessageHeap{}
	heap.Init(msgHeap)
	return &ProducerBuffer{
		ProducerID:      producerID,
		ExpectedSeq:     1, // Start from 1
		PendingMessages: msgHeap,
		WindowSize:      windowSize,
		TimeoutDuration: timeout,
		LastActivity:    time.Now(),
	}
}

// ProcessMessage attempts to process a message, returns ready messages in order
func (pb *ProducerBuffer) ProcessMessage(seqNum int64, data interface{}) ([]*PendingMessage, error) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.LastActivity = time.Now()

	// Check if message is too old,already processed
	if seqNum < pb.ExpectedSeq {
		return nil, fmt.Errorf("duplicate or old message: seq=%d, expected=%d", seqNum, pb.ExpectedSeq)
	}

	// Check if message is outside window
	if seqNum > pb.ExpectedSeq+int64(pb.WindowSize) {
		return nil, fmt.Errorf("message outside window: seq=%d, expected=%d, window=%d", 
			seqNum, pb.ExpectedSeq, pb.WindowSize)
	}

	var readyMessages []*PendingMessage

	if seqNum == pb.ExpectedSeq {
		readyMessages = append(readyMessages, &PendingMessage{
			SequenceNumber: seqNum,
			Data:           data,
			Timestamp:      time.Now(),
		})
		pb.ExpectedSeq++

		// Check if any pending messages can now be processed
		for pb.PendingMessages.Len() > 0 {
			next := (*pb.PendingMessages)[0]
			if next.SequenceNumber == pb.ExpectedSeq {
				heap.Pop(pb.PendingMessages)
				readyMessages = append(readyMessages, next)
				pb.ExpectedSeq++
			} else {
				break
			}
		}
	} else {
		// Buffer the out-of-order message
		heap.Push(pb.PendingMessages, &PendingMessage{
			SequenceNumber: seqNum,
			Data:           data,
			Timestamp:      time.Now(),
		})
	}

	return readyMessages, nil
}

// GetTimeoutMessages returns messages that have been waiting too long
func (pb *ProducerBuffer) GetTimeoutMessages() []*PendingMessage {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	now := time.Now()
	var timeoutMessages []*PendingMessage

	for _, msg := range *pb.PendingMessages {
		if now.Sub(msg.Timestamp) > pb.TimeoutDuration {
			timeoutMessages = append(timeoutMessages, msg)
		}
	}

	return timeoutMessages
}

// ForceFlush forces processing of all pending messages (for timeout handling)
func (pb *ProducerBuffer) ForceFlush() []*PendingMessage {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	var flushedMessages []*PendingMessage
	for pb.PendingMessages.Len() > 0 {
		msg := heap.Pop(pb.PendingMessages).(*PendingMessage)
		flushedMessages = append(flushedMessages, msg)
		if msg.SequenceNumber >= pb.ExpectedSeq {
			pb.ExpectedSeq = msg.SequenceNumber + 1
		}
	}

	return flushedMessages
}

// GetStats returns buffer statistics
func (pb *ProducerBuffer) GetStats() map[string]interface{} {
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	return map[string]interface{}{
		"producer_id":      pb.ProducerID,
		"expected_seq":     pb.ExpectedSeq,
		"pending_count":    pb.PendingMessages.Len(),
		"window_size":      pb.WindowSize,
		"last_activity":    pb.LastActivity,
		"timeout_duration": pb.TimeoutDuration,
	}
}