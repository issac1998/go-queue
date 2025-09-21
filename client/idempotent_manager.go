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
