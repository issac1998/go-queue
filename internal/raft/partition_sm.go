package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplicator"
	typederrors "github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/ordering"
	"github.com/issac1998/go-queue/internal/protocol"
	"github.com/issac1998/go-queue/internal/storage"
	"github.com/lni/dragonboat/v3/statemachine"
)

// ProduceMessage represents a message to be produced
type ProduceMessage struct {
	ProducerID     string            `json:"producer_id,omitempty"`
	SequenceNumber int64             `json:"sequence_number,omitempty"`
	AsyncIO        bool              `json:"async_io,omitempty"`
	Topic          string            `json:"topic"`
	Partition      int32             `json:"partition"`
	Key            []byte            `json:"key,omitempty"`
	Value          []byte            `json:"value"`
	Headers        map[string]string `json:"headers,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
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

// BatchFetchRequest represents a batch request to read multiple ranges of messages
type BatchFetchRequest struct {
	Topic     string              `json:"topic"`
	Partition int32               `json:"partition"`
	Requests  []FetchRangeRequest `json:"requests"`
}

// FetchRangeRequest represents a single range request within a batch
type FetchRangeRequest struct {
	Offset   int64 `json:"offset"`
	MaxBytes int32 `json:"max_bytes"`
	MaxCount int32 `json:"max_count,omitempty"`
}

// FetchResponse represents the response to a fetch request
type FetchResponse struct {
	Topic      string          `json:"topic"`
	Partition  int32           `json:"partition"`
	Messages   []StoredMessage `json:"messages"`
	NextOffset int64           `json:"next_offset"`
	ErrorCode  int16           `json:"error_code"`
}

// BatchFetchResponse represents the response to a batch fetch request
type BatchFetchResponse struct {
	Topic     string        `json:"topic"`
	Partition int32         `json:"partition"`
	Results   []FetchResult `json:"results"`
	ErrorCode int16         `json:"error_code"`
	Error     string        `json:"error,omitempty"`
}

// FetchResult represents the result of a single fetch range
type FetchResult struct {
	Messages   []StoredMessage `json:"messages"`
	NextOffset int64           `json:"next_offset"`
	Error      string          `json:"error,omitempty"`
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
	CmdProduceBatch   = "produce_batch"
	CmdCleanup        = "cleanup"
)

// ProduceBatchCommand represents a batch of messages to be produced
type ProduceBatchCommand struct {
	Messages []ProduceMessage `json:"messages"`
}

// BatchWriteResult represents the result of a batch write operation
type BatchWriteResult struct {
	Results []WriteResult `json:"results"`
	Error   string        `json:"error,omitempty"`
}

// deduplicatorCommand represents producer state persistence operations
type deduplicatorCommand struct {
	ProducerID string          `json:"producer_id"`
	States     map[int32]int64 `json:"states,omitempty"` // partition -> last sequence number
	Timestamp  time.Time       `json:"timestamp"`
}

// deduplicatorResult represents the result of producer state operations
type deduplicatorResult struct {
	Success   bool      `json:"success"`
	Error     string    `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// PartitionStateMachine implements statemachine.IStateMachine for partition data
type PartitionStateMachine struct {
	TopicName   string
	PartitionID int32
	DataDir     string

	// Storage components
	partition *storage.Partition
	mu        sync.RWMutex

	// Message processing components
	compressor compression.Compressor

	// Deduplicator components
	DeduplicatorManager *deduplicator.DeduplicatorManager
	deduplicatorEnabled bool

	// Ordering components
	orderedMessageManager *ordering.OrderedMessageManager

	// Metrics
	messageCount int64
	bytesStored  int64
	lastWrite    time.Time
	lastRead     time.Time

	// State
	isReady bool
}

// NewPartitionStateMachine creates a state machine for a partition
func NewPartitionStateMachine(topicName string, partitionID int32, dataDir string, compressor compression.Compressor) (*PartitionStateMachine, error) {
	partitionDir := filepath.Join(dataDir, "partitions", fmt.Sprintf("%s-%d", topicName, partitionID))

	// Create storage partition
	partition, err := storage.NewPartition(partitionDir, &storage.PartitionConfig{
		MaxSegmentSize: 1024 * 1024 * 1024,
		MaxIndexSize:   1024 * 1024 * 10,
		RetentionTime:  7 * 24 * time.Hour,
		RetentionSize:  10 * 1024 * 1024 * 1024,
	})
	if err != nil {
		return nil, typederrors.NewTypedError(typederrors.StorageError, "failed to create partition storage", err)
	}

	psm := &PartitionStateMachine{
		TopicName:             topicName,
		PartitionID:           partitionID,
		DataDir:               dataDir,
		partition:             partition,
		compressor:            compressor,
		DeduplicatorManager:   deduplicator.NewDeduplicatorManager(),
		deduplicatorEnabled:   true,
		orderedMessageManager: ordering.NewOrderedMessageManager(100, 30*time.Second), // 100 message window, 30s timeout
		isReady:               true,
		lastWrite:             time.Now(),
		lastRead:              time.Now(),
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
		return statemachine.Result{Value: 0}, typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal partition command", err)
	}

	switch cmd.Type {
	case CmdProduceMessage:
		return psm.handleProduceMessage(cmd.Data)
	case CmdProduceBatch:
		return psm.handleProduceBatch(cmd.Data)
	case CmdCleanup:
		return psm.handleCleanup(cmd.Data)
	default:
		err := fmt.Errorf("unknown command type: %s", cmd.Type)
		log.Printf("PartitionStateMachine error: %v", err)
		return statemachine.Result{Value: 0}, err
	}
}

// handleProduceMessage handles message production with ordering support
func (psm *PartitionStateMachine) handleProduceMessage(data map[string]interface{}) (statemachine.Result, error) {
	// Parse message data
	var msg ProduceMessage
	msgBytes, err := json.Marshal(data["message"])
	if err != nil {
		return statemachine.Result{Value: 0}, typederrors.NewTypedError(typederrors.GeneralError, "failed to marshal message", err)
	}

	if err := json.Unmarshal(msgBytes, &msg); err != nil {
		return statemachine.Result{Value: 0}, typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal message", err)
	}

	// Handle messages with sequence numbers using ordered processing (only for async IO producers)
	if psm.deduplicatorEnabled && msg.ProducerID != "" && msg.SequenceNumber > 0 && msg.AsyncIO {
		deduplicator := psm.DeduplicatorManager.GetOrCreatededuplicator(msg.ProducerID)

		// Check for duplicate messages first
		if deduplicator.IsDuplicateSequenceNumber(psm.PartitionID, msg.SequenceNumber, msg.AsyncIO) {
			log.Printf("Duplicate sequence number %d for producer %s on partition %d",
				msg.SequenceNumber, msg.ProducerID, psm.PartitionID)

			lastOffset := deduplicator.GetLastSequenceNumber(psm.PartitionID)
			result := WriteResult{
				Offset:    lastOffset,
				Timestamp: msg.Timestamp,
			}
			resultBytes, _ := json.Marshal(result)
			return statemachine.Result{
				Value: uint64(lastOffset),
				Data:  resultBytes,
			}, nil
		}

		// Validate sequence number
		if !deduplicator.IsValidSequenceNumber(psm.PartitionID, msg.SequenceNumber, msg.AsyncIO) {

			result := WriteResult{
				Error: fmt.Sprintf("Invalid sequence number %d for AsyncIO producer %s",
					msg.SequenceNumber, msg.ProducerID),
			}
			resultBytes, _ := json.Marshal(result)
			return statemachine.Result{
				Value: 0,
				Data:  resultBytes,
			}, nil
		}

		if msg.AsyncIO {
			readyMessages, err := psm.orderedMessageManager.ProcessMessage(
				psm.PartitionID, msg.ProducerID, msg.SequenceNumber, &msg)
			if err != nil {
				// Message is outside window or other ordering error,nor a really error
				result := WriteResult{
					Error: fmt.Sprintf("Ordering error for producer %s seq %d: %v",
						msg.ProducerID, msg.SequenceNumber, err),
				}
				resultBytes, _ := json.Marshal(result)
				return statemachine.Result{
					Value: 0,
					Data:  resultBytes,
				}, nil
			}

			if len(readyMessages) > 0 {
				return psm.processOrderedMessages(readyMessages)
			} else {
				// this is not a error
				result := WriteResult{
					Offset:    -1,
					Timestamp: msg.Timestamp,
				}
				resultBytes, _ := json.Marshal(result)
				return statemachine.Result{
					Value: 0,
					Data:  resultBytes,
				}, nil
			}
		}
	}

	// non-AsyncIO messages
	if psm.deduplicatorEnabled && msg.ProducerID != "" && msg.SequenceNumber > 0 && !msg.AsyncIO {
		deduplicator := psm.DeduplicatorManager.GetOrCreatededuplicator(msg.ProducerID)

		// Check for duplicate messages first
		if deduplicator.IsDuplicateSequenceNumber(psm.PartitionID, msg.SequenceNumber, msg.AsyncIO) {
			log.Printf("Duplicate sequence number %d for producer %s on partition %d",
				msg.SequenceNumber, msg.ProducerID, psm.PartitionID)

			lastOffset := deduplicator.GetLastSequenceNumber(psm.PartitionID)
			result := WriteResult{
				Offset:    lastOffset,
				Timestamp: msg.Timestamp,
			}
			resultBytes, _ := json.Marshal(result)
			return statemachine.Result{
				Value: uint64(lastOffset),
				Data:  resultBytes,
			}, nil
		}

		// Validate sequence number for non-AsyncIO (must be strictly sequential)
		if !deduplicator.IsValidSequenceNumber(psm.PartitionID, msg.SequenceNumber, msg.AsyncIO) {
			log.Printf("Invalid sequence number %d for producer %s on partition %d, expected %d",
				msg.SequenceNumber, msg.ProducerID, psm.PartitionID,
				deduplicator.GetLastSequenceNumber(psm.PartitionID)+1)

			result := WriteResult{
				Error: fmt.Sprintf("Invalid sequence number %d, expected %d",
					msg.SequenceNumber, deduplicator.GetLastSequenceNumber(psm.PartitionID)+1),
			}
			resultBytes, _ := json.Marshal(result)
			return statemachine.Result{
				Value: 0,
				Data:  resultBytes,
			}, nil
		}
	}

	// Store message
	offset, err := psm.storeMessage(&msg)
	if err != nil {
		return statemachine.Result{Value: 0}, typederrors.NewTypedError(typederrors.StorageError, "failed to store message", err)
	}

	// Update producer state after successful write for deduplicator
	if psm.deduplicatorEnabled && msg.ProducerID != "" && msg.SequenceNumber > 0 {
		deduplicator := psm.DeduplicatorManager.GetOrCreatededuplicator(msg.ProducerID)
		deduplicator.UpdateSequenceNumber(psm.PartitionID, msg.SequenceNumber)
	}

	result := WriteResult{
		Offset:    offset,
		Timestamp: msg.Timestamp,
	}

	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Printf("Failed to marshal write result: %v", err)
		resultBytes = []byte(fmt.Sprintf(`{"offset":%d,"timestamp":"%s"}`, offset, msg.Timestamp.Format(time.RFC3339)))
	}

	return statemachine.Result{
		Value: uint64(offset),
		Data:  resultBytes,
	}, nil
}

func (psm *PartitionStateMachine) processOrderedMessages(readyMessages []*ordering.PendingMessage) (statemachine.Result, error) {
	var results []WriteResult
	lastOffset := int64(-1)

	for _, pendingMsg := range readyMessages {
		msg, ok := pendingMsg.Data.(*ProduceMessage)
		if !ok {
			return statemachine.Result{Value: 0}, typederrors.NewTypedError(typederrors.GeneralError, "invalid message type in ordered processing", nil)
		}

		offset, err := psm.storeMessage(msg)
		if err != nil {
			return statemachine.Result{Value: 0}, typederrors.NewTypedError(typederrors.StorageError, "failed to store ordered message", err)
		}

		if psm.deduplicatorEnabled && msg.ProducerID != "" {
			deduplicator := psm.DeduplicatorManager.GetOrCreatededuplicator(msg.ProducerID)
			deduplicator.UpdateSequenceNumber(psm.PartitionID, msg.SequenceNumber)
		}

		results = append(results, WriteResult{
			Offset:    offset,
			Timestamp: msg.Timestamp,
		})
		lastOffset = offset
	}

	if len(results) > 0 {
		resultBytes, _ := json.Marshal(results[len(results)-1])
		return statemachine.Result{
			Value: uint64(lastOffset),
			Data:  resultBytes,
		}, nil
	}

	return statemachine.Result{Value: 0}, typederrors.NewTypedError(typederrors.GeneralError, "no messages processed", nil)
}

func (psm *PartitionStateMachine) storeMessage(msg *ProduceMessage) (int64, error) {
	messageData := StoredMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
		Timestamp: msg.Timestamp,
	}

	serializedMsg, err := json.Marshal(messageData)
	if err != nil {
		return 0, typederrors.NewTypedError(typederrors.GeneralError, "failed to serialize message", err)
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
		return 0, typederrors.NewTypedError(typederrors.StorageError, "failed to append message", err)
	}

	psm.messageCount++
	psm.bytesStored += int64(len(serializedMsg))
	psm.lastWrite = time.Now()

	return offset, nil
}

func (psm *PartitionStateMachine) handleProduceBatch(data map[string]interface{}) (statemachine.Result, error) {
	var batchCmd ProduceBatchCommand
	batchCmdBytes, err := json.Marshal(data["batch"])
	if err != nil {
		return statemachine.Result{Value: protocol.ErrorInvalidRequest}, typederrors.NewTypedError(typederrors.GeneralError, "failed to marshal batch command", err)
	}

	if err := json.Unmarshal(batchCmdBytes, &batchCmd); err != nil {
		return statemachine.Result{Value: protocol.ErrorInvalidRequest}, typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal batch command", err)
	}

	results := make([]WriteResult, len(batchCmd.Messages))
	for i, msg := range batchCmd.Messages {
		if psm.deduplicatorEnabled && msg.ProducerID != "" && msg.SequenceNumber > 0 {
			deduplicator := psm.DeduplicatorManager.GetOrCreatededuplicator(msg.ProducerID)
			if deduplicator.IsDuplicateSequenceNumber(psm.PartitionID, msg.SequenceNumber, msg.AsyncIO) {
				lastOffset := deduplicator.GetLastSequenceNumber(psm.PartitionID)
				results[i] = WriteResult{
					Offset:    lastOffset,
					Timestamp: msg.Timestamp,
				}
				log.Printf("Duplicate sequence number %d detected for producer %s in batch message %d", msg.SequenceNumber, msg.ProducerID, i)
				continue
			}

			if !deduplicator.IsValidSequenceNumber(psm.PartitionID, msg.SequenceNumber, msg.AsyncIO) {
				results[i] = WriteResult{Error: fmt.Sprintf("invalid sequence number %d for producer %s in message %d", msg.SequenceNumber, msg.ProducerID, i)}
				continue
			}
		}

		messageData := StoredMessage{
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   msg.Headers,
			Timestamp: msg.Timestamp,
		}

		serializedMsg, err := json.Marshal(messageData)
		if err != nil {
			results[i] = WriteResult{Error: fmt.Sprintf("failed to serialize message %d: %v", i, err)}
			continue
		}

		var finalMsg []byte
		if psm.compressor != nil && psm.compressor.Type() != compression.None {
			if len(serializedMsg) >= 1024 {
				compressedMsg, err := psm.compressor.Compress(serializedMsg)
				if err != nil {
					results[i] = WriteResult{Error: fmt.Sprintf("compression failed for message %d: %v", i, err)}
					log.Printf("Compression failed for message %d, storing uncompressed: %v", i, err)
					finalMsg = serializedMsg
				} else {
					finalMsg = make([]byte, 1+len(compressedMsg))
					finalMsg[0] = byte(psm.compressor.Type())
					copy(finalMsg[1:], compressedMsg)

					compressionRatio := float64(len(compressedMsg)) / float64(len(serializedMsg))
					log.Printf("Message %d compressed: %d -> %d bytes (ratio: %.2f)",
						i, len(serializedMsg), len(compressedMsg), compressionRatio)
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
			results[i] = WriteResult{Error: fmt.Sprintf("failed to append message %d: %v", i, err)}
			log.Printf("Failed to append message %d: %v", i, err)
			continue
		}

		if psm.deduplicatorEnabled && msg.ProducerID != "" {
			deduplicator := psm.DeduplicatorManager.GetOrCreatededuplicator(msg.ProducerID)
			deduplicator.UpdateSequenceNumber(psm.PartitionID, msg.SequenceNumber)
		}

		psm.messageCount++
		psm.bytesStored += int64(len(serializedMsg))
		psm.lastWrite = time.Now()

		results[i] = WriteResult{
			Offset:    offset,
			Timestamp: msg.Timestamp,
		}
		log.Printf("Produced message %d to %s-%d at offset %d", i, psm.TopicName, psm.PartitionID, offset)
	}

	batchResult := BatchWriteResult{Results: results}
	batchResultBytes, err := json.Marshal(batchResult)
	if err != nil {
		log.Printf("Failed to marshal batch write result: %v", err)
		batchResultBytes = []byte(`{"results":[],"error":"failed to marshal results"}`)
	}

	log.Printf("Produced batch of %d messages to %s-%d", len(batchCmd.Messages), psm.TopicName, psm.PartitionID)

	return statemachine.Result{
		Value: 0,
		Data:  batchResultBytes,
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

	// Try to parse as BatchFetchRequest first
	var batchReq BatchFetchRequest
	if err := json.Unmarshal(queryBytes, &batchReq); err == nil && len(batchReq.Requests) > 0 {
		return psm.handleBatchFetchMessages(&batchReq)
	}

	var req FetchRequest
	if err := json.Unmarshal(queryBytes, &req); err != nil {
		return nil, typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal fetch request", err)
	}

	return psm.handleFetchMessages(&req)
}

// handleBatchFetchMessages handles batch message fetching
func (psm *PartitionStateMachine) handleBatchFetchMessages(req *BatchFetchRequest) (*BatchFetchResponse, error) {
	if req.Topic != psm.TopicName || req.Partition != psm.PartitionID {
		return &BatchFetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Results:   []FetchResult{},
			ErrorCode: protocol.ErrorInvalidTopic,
			Error:     "topic or partition mismatch",
		}, nil
	}

	if len(req.Requests) == 0 {
		return &BatchFetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Results:   []FetchResult{},
			ErrorCode: 2, // Invalid request
			Error:     "empty batch request",
		}, nil
	}

	results := make([]FetchResult, len(req.Requests))
	for i, fetchRange := range req.Requests {
		messages, nextOffset, err := psm.readMessagesFromStorageWithCount(
			fetchRange.Offset,
			fetchRange.MaxBytes,
			fetchRange.MaxCount,
		)

		if err != nil {
			results[i] = FetchResult{
				Messages:   []StoredMessage{},
				NextOffset: fetchRange.Offset,
				Error:      fmt.Sprintf("failed to read range %d: %v", i, err),
			}
			log.Printf("Failed to read messages for range %d: %v", i, err)
		} else {
			results[i] = FetchResult{
				Messages:   messages,
				NextOffset: nextOffset,
			}
		}
	}

	psm.lastRead = time.Now()

	log.Printf("✅ Batch fetched %d ranges from %s-%d",
		len(req.Requests), psm.TopicName, psm.PartitionID)

	return &BatchFetchResponse{
		Topic:     req.Topic,
		Partition: req.Partition,
		Results:   results,
		ErrorCode: protocol.ErrorNone,
	}, nil
}

// handleFetchMessages handles message fetching
func (psm *PartitionStateMachine) handleFetchMessages(req *FetchRequest) (*FetchResponse, error) {
	if req.Topic != psm.TopicName || req.Partition != psm.PartitionID {
		return &FetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Messages:  []StoredMessage{},
			ErrorCode: protocol.ErrorInvalidTopic,
		}, nil
	}

	messages, nextOffset, err := psm.readMessagesFromStorage(req.Offset, req.MaxBytes)
	if err != nil {
		log.Printf("Failed to read messages: %v", err)
		return &FetchResponse{
			Topic:     req.Topic,
			Partition: req.Partition,
			Messages:  []StoredMessage{},
			ErrorCode: protocol.ErrorFetchFailed,
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
		ErrorCode:  protocol.ErrorNone,
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

// readMessagesFromStorageWithCount reads messages from the storage layer with both bytes and count limits
func (psm *PartitionStateMachine) readMessagesFromStorageWithCount(startOffset int64, maxBytes int32, maxCount int32) ([]StoredMessage, int64, error) {
	messages := []StoredMessage{}
	currentOffset := startOffset
	totalBytes := int32(0)

	if maxCount <= 0 {
		maxCount = 1000
	}

	for totalBytes < maxBytes && int32(len(messages)) < maxCount {
		messageData, err := psm.partition.ReadAt(currentOffset)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				break
			}
			return nil, currentOffset, err
		}

		var actualMessageData []byte
		if len(messageData) > 0 {
			compressionType := compression.CompressionType(messageData[0])
			if compressionType != compression.None {
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
				actualMessageData = messageData[1:]
			}
		} else {
			actualMessageData = messageData
		}

		var msg StoredMessage
		if err := json.Unmarshal(actualMessageData, &msg); err != nil {
			log.Printf("Failed to unmarshal stored message at offset %d: %v", currentOffset, err)
			currentOffset++
			continue
		}

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

	// Get producer states for persistence
	deduplicators := psm.DeduplicatorManager.GetAlldeduplicators()

	// Create snapshot metadata
	snapshot := map[string]interface{}{
		"topic_name":      psm.TopicName,
		"partition_id":    psm.PartitionID,
		"message_count":   psm.messageCount,
		"bytes_stored":    psm.bytesStored,
		"last_write":      psm.lastWrite,
		"last_read":       psm.lastRead,
		"producer_states": deduplicators,
	}

	// Write snapshot metadata
	snapshotBytes, err := json.Marshal(snapshot)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to marshal snapshot", err)
	}

	if _, err := w.Write(snapshotBytes); err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to write snapshot", err)
	}

	log.Printf("Saved snapshot for %s-%d with %d producer states", psm.TopicName, psm.PartitionID, len(deduplicators))
	return nil
}

// RecoverFromSnapshot implements statemachine.IStateMachine interface
func (psm *PartitionStateMachine) RecoverFromSnapshot(r io.Reader, files []statemachine.SnapshotFile, done <-chan struct{}) error {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	// Read snapshot data
	snapshotBytes, err := io.ReadAll(r)
	if err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to read snapshot", err)
	}

	var snapshot map[string]interface{}
	if err := json.Unmarshal(snapshotBytes, &snapshot); err != nil {
		return typederrors.NewTypedError(typederrors.GeneralError, "failed to unmarshal snapshot", err)
	}

	// Restore state
	if mc, ok := snapshot["message_count"].(float64); ok {
		psm.messageCount = int64(mc)
	}
	if bs, ok := snapshot["bytes_stored"].(float64); ok {
		psm.bytesStored = int64(bs)
	}

	if deduplicatorsData, ok := snapshot["producer_states"]; ok {
		if err := psm.restorededuplicators(deduplicatorsData); err != nil {
			log.Printf("Failed to restore producer states: %v", err)
		}
	}

	log.Printf("Recovered from snapshot for %s-%d", psm.TopicName, psm.PartitionID)
	return nil
}

func (psm *PartitionStateMachine) restorededuplicators(data interface{}) error {
	statesMap, ok := data.(map[string]interface{})
	if !ok {
		return typederrors.NewTypedError(typederrors.GeneralError, "invalid producer states data type", nil)
	}

	restoredCount := 0
	for producerID, stateData := range statesMap {
		stateMap, ok := stateData.(map[string]interface{})
		if !ok {
			continue
		}

		deduplicator := deduplicator.NewDeduplicator(producerID)

		if lastSeqData, exists := stateMap["last_sequence_num"]; exists {
			if lastSeqMap, ok := lastSeqData.(map[string]interface{}); ok {
				for partStr, seqVal := range lastSeqMap {
					if partition, err := strconv.ParseInt(partStr, 10, 32); err == nil {
						if seqNum, ok := seqVal.(float64); ok {
							deduplicator.UpdateSequenceNumber(int32(partition), int64(seqNum))
						}
					}
				}
			}
		}

		psm.DeduplicatorManager.Setdeduplicator(producerID, deduplicator)
		restoredCount++
	}

	log.Printf("Restored %d producer states for partition %s-%d", restoredCount, psm.TopicName, psm.PartitionID)
	return nil
}

func (psm *PartitionStateMachine) Close() error {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	// Close ordered message manager
	if psm.orderedMessageManager != nil {
		psm.orderedMessageManager.Close()
	}

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
