package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplication"
	"github.com/issac1998/go-queue/internal/storage"
	"github.com/lni/dragonboat/v3/statemachine"
)

// ProduceMessage represents a message to be produced
type ProduceMessage struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// WriteResult represents the result of a write operation
type WriteResult struct {
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Error     string    `json:"error,omitempty"`
}

// FetchRequest represents a request to read messages
type FetchRequest struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxBytes  int32  `json:"max_bytes"`
}

// FetchResponse represents the response to a fetch request
type FetchResponse struct {
	Topic      string          `json:"topic"`
	Partition  int32           `json:"partition"`
	Messages   []StoredMessage `json:"messages"`
	NextOffset int64           `json:"next_offset"`
	ErrorCode  int16           `json:"error_code"`
}

// StoredMessage represents a message stored in the partition
type StoredMessage struct {
	Offset    int64             `json:"offset"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// PartitionCommand represents different types of operations on a partition
type PartitionCommand struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

// Command types for partition operations
const (
	CmdProduceMessage = "produce_message"
	CmdCleanup        = "cleanup"
)

// PartitionStateMachine implements statemachine.IStateMachine for partition data
type PartitionStateMachine struct {
	TopicName   string
	PartitionID int32
	DataDir     string

	// Storage components
	partition *storage.Partition
	mu        sync.RWMutex

	// Message processing components
	compressor   compression.Compressor
	deduplicator *deduplication.Deduplicator

	// Metrics
	messageCount int64
	bytesStored  int64
	lastWrite    time.Time
	lastRead     time.Time

	// State
	isReady bool
}

// NewPartitionStateMachine creates a state machine for a partition
func NewPartitionStateMachine(topicName string, partitionID int32, dataDir string, compressor compression.Compressor, deduplicator *deduplication.Deduplicator) (*PartitionStateMachine, error) {
	partitionDir := filepath.Join(dataDir, "partitions", fmt.Sprintf("%s-%d", topicName, partitionID))

	// Create storage partition
	partition, err := storage.NewPartition(partitionDir, &storage.PartitionConfig{
		MaxSegmentSize: 1024 * 1024 * 1024,
		MaxIndexSize:   1024 * 1024 * 10,
		RetentionTime:  7 * 24 * time.Hour,
		RetentionSize:  10 * 1024 * 1024 * 1024,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create partition storage: %w", err)
	}

	psm := &PartitionStateMachine{
		TopicName:    topicName,
		PartitionID:  partitionID,
		DataDir:      dataDir,
		partition:    partition,
		compressor:   compressor,
		deduplicator: deduplicator,
		isReady:      true,
		lastWrite:    time.Now(),
		lastRead:     time.Now(),
	}

	log.Printf("Created PartitionStateMachine for %s-%d", topicName, partitionID)
	return psm, nil
}

// Update implements statemachine.IStateMachine interface
// This is called when Raft log entries are applied
func (psm *PartitionStateMachine) Update(data []byte) (statemachine.Result, error) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	var cmd PartitionCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		log.Printf("Failed to unmarshal partition command: %v", err)
		return statemachine.Result{Value: 0}, err
	}

	switch cmd.Type {
	case CmdProduceMessage:
		return psm.handleProduceMessage(cmd.Data)
	case CmdCleanup:
		return psm.handleCleanup(cmd.Data)
	default:
		err := fmt.Errorf("unknown command type: %s", cmd.Type)
		log.Printf("PartitionStateMachine error: %v", err)
		return statemachine.Result{Value: 0}, err
	}
}

// handleProduceMessage handles message production
func (psm *PartitionStateMachine) handleProduceMessage(data map[string]interface{}) (statemachine.Result, error) {
	// Parse message data
	var msg ProduceMessage
	msgBytes, err := json.Marshal(data["message"])
	if err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to marshal message: %w", err)
	}

	if err := json.Unmarshal(msgBytes, &msg); err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Create stored message
	messageData := StoredMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
		Timestamp: msg.Timestamp,
	}

	// Serialize message for storage
	serializedMsg, err := json.Marshal(messageData)
	if err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to serialize message: %w", err)
	}

	if psm.deduplicator != nil && psm.deduplicator.IsEnabled() {
		isDuplicate, existingOffset, err := psm.deduplicator.IsDuplicate(serializedMsg, -1)
		if err != nil {
			log.Printf("Deduplication check failed: %v", err)
		} else if isDuplicate {
			log.Printf("Duplicate message detected for %s-%d, returning existing offset %d",
				psm.TopicName, psm.PartitionID, existingOffset)

			result := WriteResult{
				Offset:    existingOffset,
				Timestamp: msg.Timestamp,
			}

			resultBytes, _ := json.Marshal(result)
			return statemachine.Result{
				Value: uint64(existingOffset),
				Data:  resultBytes,
			}, nil
		}
	}

	var finalMsg []byte
	if psm.compressor != nil && psm.compressor.Type() != compression.None {
		if len(serializedMsg) >= 1024 {
			compressedMsg, err := psm.compressor.Compress(serializedMsg)
			if err != nil {
				log.Printf("Compression failed, storing uncompressed: %v", err)
				finalMsg = serializedMsg
			} else {
				finalMsg = make([]byte, 1+len(compressedMsg))
				finalMsg[0] = byte(psm.compressor.Type())
				copy(finalMsg[1:], compressedMsg)

				compressionRatio := float64(len(compressedMsg)) / float64(len(serializedMsg))
				log.Printf("Message compressed: %d -> %d bytes (ratio: %.2f)",
					len(serializedMsg), len(compressedMsg), compressionRatio)
			}
		} else {
			finalMsg = make([]byte, 1+len(serializedMsg))
			finalMsg[0] = byte(compression.None)
			copy(finalMsg[1:], serializedMsg)
		}
	} else {
		finalMsg = make([]byte, 1+len(serializedMsg))
		finalMsg[0] = byte(compression.None)
		copy(finalMsg[1:], serializedMsg)
	}

	offset, err := psm.partition.Append(finalMsg, msg.Timestamp)
	if err != nil {
		return statemachine.Result{Value: 0}, fmt.Errorf("failed to append message: %w", err)
	}

	if psm.deduplicator != nil && psm.deduplicator.IsEnabled() {
		_, _, err := psm.deduplicator.IsDuplicate(serializedMsg, offset)
		if err != nil {
			log.Printf("Failed to update deduplication index: %v", err)
		}
	}

	psm.messageCount++
	psm.bytesStored += int64(len(serializedMsg))
	psm.lastWrite = time.Now()

	result := WriteResult{
		Offset:    offset,
		Timestamp: msg.Timestamp,
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("Failed to marshal write result: %v", err)
		resultBytes = []byte(fmt.Sprintf(`{"offset":%d,"timestamp":"%s"}`, offset, msg.Timestamp.Format(time.RFC3339)))
	}

	log.Printf("Produced message to %s-%d at offset %d", psm.TopicName, psm.PartitionID, offset)

	return statemachine.Result{
		Value: uint64(offset),
		Data:  resultBytes,
	}, nil
}

// handleCleanup handles partition cleanup operations
func (psm *PartitionStateMachine) handleCleanup(data map[string]interface{}) (statemachine.Result, error) {
	// This would handle cleanup operations like log compaction
	log.Printf("Performed cleanup on %s-%d", psm.TopicName, psm.PartitionID)

	result := map[string]interface{}{
		"status": "success",
		"type":   "cleanup",
	}

	resultBytes, _ := json.Marshal(result)
	return statemachine.Result{
		Value: 1,
		Data:  resultBytes,
	}, nil
}

// Lookup implements statemachine.IStateMachine interface
// This is used for read operations (queries)
func (psm *PartitionStateMachine) Lookup(query interface{}) (interface{}, error) {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	// Convert query to bytes if needed
	var queryBytes []byte
	switch q := query.(type) {
	case []byte:
		queryBytes = q
	case string:
		queryBytes = []byte(q)
	default:
		return nil, fmt.Errorf("invalid query type: %T", query)
	}

	var req FetchRequest
	if err := json.Unmarshal(queryBytes, &req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal fetch request: %w", err)
	}

	return psm.handleFetchMessages(&req)
}

// handleFetchMessages handles message fetching
func (psm *PartitionStateMachine) handleFetchMessages(req *FetchRequest) (*FetchResponse, error) {
	if req.Topic != psm.TopicName || req.Partition != psm.PartitionID {
		return &FetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Messages:  []StoredMessage{},
			ErrorCode: 1, // Invalid topic/partition
		}, nil
	}

	messages, nextOffset, err := psm.readMessagesFromStorage(req.Offset, req.MaxBytes)
	if err != nil {
		log.Printf("Failed to read messages: %v", err)
		return &FetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Messages:  []StoredMessage{},
			ErrorCode: 2, // Read error
		}, nil
	}

	psm.lastRead = time.Now()

	log.Printf("Fetched %d messages from %s-%d starting at offset %d",
		len(messages), psm.TopicName, psm.PartitionID, req.Offset)

	return &FetchResponse{
		Topic:      req.Topic,
		Partition:  req.Partition,
		Messages:   messages,
		NextOffset: nextOffset,
		ErrorCode:  0, // Success
	}, nil
}

// readMessagesFromStorage reads messages from the storage layer
func (psm *PartitionStateMachine) readMessagesFromStorage(startOffset int64, maxBytes int32) ([]StoredMessage, int64, error) {
	messages := []StoredMessage{}
	currentOffset := startOffset
	totalBytes := int32(0)

	// Read messages until we hit the limit or run out of data
	for totalBytes < maxBytes && len(messages) < 1000 { // Max 1000 messages per fetch
		messageData, err := psm.partition.ReadAt(currentOffset)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				break // No more messages
			}
			return nil, currentOffset, err
		}

		var actualMessageData []byte
		if len(messageData) > 0 {
			compressionType := compression.CompressionType(messageData[0])
			if compressionType != compression.None {
				// Message is compressed, decompress it
				compressor, err := compression.GetCompressor(compressionType)
				if err != nil {
					log.Printf("Failed to get decompressor for type %d at offset %d: %v", compressionType, currentOffset, err)
					currentOffset++
					continue
				}

				decompressedData, err := compressor.Decompress(messageData[1:])
				if err != nil {
					log.Printf("Failed to decompress message at offset %d: %v", currentOffset, err)
					currentOffset++
					continue
				}
				actualMessageData = decompressedData
			} else {
				// Message is not compressed, skip the compression marker
				actualMessageData = messageData[1:]
			}
		} else {
			actualMessageData = messageData
		}

		// Deserialize message
		var msg StoredMessage
		if err := json.Unmarshal(actualMessageData, &msg); err != nil {
			log.Printf("Failed to unmarshal stored message at offset %d: %v", currentOffset, err)
			currentOffset++
			continue
		}

		// Set the offset
		msg.Offset = currentOffset

		messages = append(messages, msg)
		totalBytes += int32(len(messageData))
		currentOffset++
	}

	return messages, currentOffset, nil
}

// SaveSnapshot implements statemachine.IStateMachine interface
func (psm *PartitionStateMachine) SaveSnapshot(w io.Writer, fc statemachine.ISnapshotFileCollection, done <-chan struct{}) error {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	// Create snapshot metadata
	snapshot := map[string]interface{}{
		"topic_name":    psm.TopicName,
		"partition_id":  psm.PartitionID,
		"message_count": psm.messageCount,
		"bytes_stored":  psm.bytesStored,
		"last_write":    psm.lastWrite,
		"last_read":     psm.lastRead,
	}

	// Write snapshot metadata
	snapshotBytes, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	if _, err := w.Write(snapshotBytes); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	log.Printf("Saved snapshot for %s-%d", psm.TopicName, psm.PartitionID)
	return nil
}

// RecoverFromSnapshot implements statemachine.IStateMachine interface
func (psm *PartitionStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	// Read snapshot data
	snapshotBytes, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot map[string]interface{}
	if err := json.Unmarshal(snapshotBytes, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// Restore state
	if mc, ok := snapshot["message_count"].(float64); ok {
		psm.messageCount = int64(mc)
	}
	if bs, ok := snapshot["bytes_stored"].(float64); ok {
		psm.bytesStored = int64(bs)
	}

	log.Printf("Recovered from snapshot for %s-%d", psm.TopicName, psm.PartitionID)
	return nil
}

// Close implements statemachine.IStateMachine interface
func (psm *PartitionStateMachine) Close() error {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	if psm.partition != nil {
		if err := psm.partition.Close(); err != nil {
			log.Printf("Failed to close partition storage: %v", err)
			return err
		}
	}

	psm.isReady = false
	log.Printf("Closed PartitionStateMachine for %s-%d", psm.TopicName, psm.PartitionID)
	return nil
}

// IsReady returns whether the partition is ready for operations
func (psm *PartitionStateMachine) IsReady() bool {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	return psm.isReady
}

// GetMetrics returns partition metrics
func (psm *PartitionStateMachine) GetMetrics() map[string]interface{} {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	return map[string]interface{}{
		"message_count": psm.messageCount,
		"bytes_stored":  psm.bytesStored,
		"last_write":    psm.lastWrite,
		"last_read":     psm.lastRead,
		"is_ready":      psm.isReady,
	}
}
