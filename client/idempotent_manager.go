package client

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// ProcessedRecord record processed message
type ProcessedRecord struct {
	MessageID     string    `json:"message_id"`
	Topic         string    `json:"topic"`
	Partition     int32     `json:"partition"`
	Offset        int64     `json:"offset"`
	ProcessedAt   time.Time `json:"processed_at"`
	TransactionID string    `json:"transaction_id,omitempty"`
	Checksum      string    `json:"checksum,omitempty"`
}

// IdempotentStorage  check message indempotent
type IdempotentStorage interface {
	IsProcessed(messageID string) (bool, error)

	MarkProcessed(record *ProcessedRecord) error

	BatchMarkProcessed(records []*ProcessedRecord) error

	GetProcessedRecord(messageID string) (*ProcessedRecord, error)
	CleanupExpired(expiration time.Duration) error

	Close() error
}

// MemoryIdempotentStorage support memory storage
type MemoryIdempotentStorage struct {
	records       map[string]*ProcessedRecord
	mu            sync.RWMutex
	maxRecords    int
	cleanupTicker *time.Ticker
	stopCleanup   chan struct{}
}

// NewMemoryIdempotentStorage create memory idempotent storage
func NewMemoryIdempotentStorage(maxRecords int, cleanupInterval time.Duration) *MemoryIdempotentStorage {
	storage := &MemoryIdempotentStorage{
		records:     make(map[string]*ProcessedRecord),
		maxRecords:  maxRecords,
		stopCleanup: make(chan struct{}),
	}

	if cleanupInterval > 0 {
		storage.cleanupTicker = time.NewTicker(cleanupInterval)
		go storage.runCleanup()
	}

	return storage
}

// IsProcessed check message is processed
func (m *MemoryIdempotentStorage) IsProcessed(messageID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.records[messageID]
	return exists, nil
}

// MarkProcessed mark message as processed
func (m *MemoryIdempotentStorage) MarkProcessed(record *ProcessedRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.records) >= m.maxRecords {
		m.evictOldest()
	}

	m.records[record.MessageID] = record
	return nil
}

// BatchMarkProcessed batch mark message as processed
func (m *MemoryIdempotentStorage) BatchMarkProcessed(records []*ProcessedRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, record := range records {
		if len(m.records) >= m.maxRecords {
			m.evictOldest()
		}
		m.records[record.MessageID] = record
	}

	return nil
}

// GetProcessedRecord get processed record
func (m *MemoryIdempotentStorage) GetProcessedRecord(messageID string) (*ProcessedRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	record, exists := m.records[messageID]
	if !exists {
		return nil, fmt.Errorf("record not found for message ID: %s", messageID)
	}

	return record, nil
}

// CleanupExpired cleanup expired record
func (m *MemoryIdempotentStorage) CleanupExpired(expiration time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for messageID, record := range m.records {
		if now.Sub(record.ProcessedAt) > expiration {
			delete(m.records, messageID)
		}
	}

	return nil
}

// Close close storage
func (m *MemoryIdempotentStorage) Close() error {
	if m.cleanupTicker != nil {
		m.cleanupTicker.Stop()
	}

	select {
	case m.stopCleanup <- struct{}{}:
	default:
	}

	return nil
}

// evictOldest evict oldest record
func (m *MemoryIdempotentStorage) evictOldest() {
	var oldestID string
	var oldestTime time.Time

	first := true
	for messageID, record := range m.records {
		if first || record.ProcessedAt.Before(oldestTime) {
			oldestID = messageID
			oldestTime = record.ProcessedAt
			first = false
		}
	}

	if oldestID != "" {
		delete(m.records, oldestID)
	}
}

// runCleanup run cleanup task
func (m *MemoryIdempotentStorage) runCleanup() {
	for {
		select {
		case <-m.cleanupTicker.C:
			m.CleanupExpired(time.Hour)
		case <-m.stopCleanup:
			return
		}
	}
}

// IdempotentManager idempotent manager
type IdempotentManager struct {
	storage IdempotentStorage
	mu      sync.RWMutex
}

// NewIdempotentManager create idempotent manager
func NewIdempotentManager(storage IdempotentStorage) *IdempotentManager {
	return &IdempotentManager{
		storage: storage,
	}
}

// GenerateMessageID generate message unique ID
func (im *IdempotentManager) GenerateMessageID(msg *ConsumeMessage) string {
	// based on message content and location generate unique ID
	hash := sha256.New()
	hash.Write([]byte(fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)))
	if msg.Key != nil {
		hash.Write(msg.Key)
	}
	hash.Write(msg.Value)
	return hex.EncodeToString(hash.Sum(nil))
}

// IsProcessed check message is processed
func (im *IdempotentManager) IsProcessed(messageID string) (bool, error) {
	return im.storage.IsProcessed(messageID)
}

// MarkProcessed mark message as processed
func (im *IdempotentManager) MarkProcessed(msg *ConsumeMessage, transactionID string) error {
	messageID := im.GenerateMessageID(msg)

	record := &ProcessedRecord{
		MessageID:     messageID,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		ProcessedAt:   time.Now(),
		TransactionID: transactionID,
		Checksum:      im.generateChecksum(msg),
	}

	return im.storage.MarkProcessed(record)
}

// BatchMarkProcessed batch mark message as processed
func (im *IdempotentManager) BatchMarkProcessed(messages []*ConsumeMessage, transactionID string) error {
	records := make([]*ProcessedRecord, len(messages))

	for i, msg := range messages {
		messageID := im.GenerateMessageID(msg)
		records[i] = &ProcessedRecord{
			MessageID:     messageID,
			Topic:         msg.Topic,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			ProcessedAt:   time.Now(),
			TransactionID: transactionID,
			Checksum:      im.generateChecksum(msg),
		}
	}

	return im.storage.BatchMarkProcessed(records)
}

// GetProcessedRecord get processed record
func (im *IdempotentManager) GetProcessedRecord(messageID string) (*ProcessedRecord, error) {
	return im.storage.GetProcessedRecord(messageID)
}

// Close close idempotent manager
func (im *IdempotentManager) Close() error {
	return im.storage.Close()
}

// generateChecksum generate message checksum
func (im *IdempotentManager) generateChecksum(msg *ConsumeMessage) string {
	hash := sha256.New()
	if msg.Key != nil {
		hash.Write(msg.Key)
	}
	hash.Write(msg.Value)
	return hex.EncodeToString(hash.Sum(nil))[:16] // take first 16 bits as checksum
}
