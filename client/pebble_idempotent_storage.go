package client

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleIdempotentStorage implements IdempotentStorage using PebbleDB
type PebbleIdempotentStorage struct {
	db     *pebble.DB
	dbPath string
}

// NewPebbleIdempotentStorage creates a new PebbleDB-backed idempotent storage
func NewPebbleIdempotentStorage(dbPath string) (*PebbleIdempotentStorage, error) {
	opts := &pebble.Options{
		Cache:                 pebble.NewCache(64 << 20), // 64MB cache
		MemTableSize:          4 << 20,                   // 4MB memtable
		MemTableStopWritesThreshold: 2,
		LBaseMaxBytes:         64 << 20, // 64MB
		Levels: make([]pebble.LevelOptions, 7),
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB at %s: %w", dbPath, err)
	}

	return &PebbleIdempotentStorage{
		db:     db,
		dbPath: dbPath,
	}, nil
}

// IsProcessed checks if a message has been processed
func (p *PebbleIdempotentStorage) IsProcessed(messageID string) (bool, error) {
	key := []byte(messageID)
	
	_, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if message is processed: %w", err)
	}
	
	closer.Close()
	return true, nil
}

// MarkProcessed marks a message as processed
func (p *PebbleIdempotentStorage) MarkProcessed(record *ProcessedRecord) error {
	key := []byte(record.MessageID)
	
	value, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal processed record: %w", err)
	}
	
	if err := p.db.Set(key, value, pebble.Sync); err != nil {
		return fmt.Errorf("failed to mark message as processed: %w", err)
	}
	
	return nil
}

// BatchMarkProcessed marks multiple messages as processed in a batch
func (p *PebbleIdempotentStorage) BatchMarkProcessed(records []*ProcessedRecord) error {
	batch := p.db.NewBatch()
	defer batch.Close()
	
	for _, record := range records {
		key := []byte(record.MessageID)
		
		value, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal processed record for message %s: %w", record.MessageID, err)
		}
		
		if err := batch.Set(key, value, nil); err != nil {
			return fmt.Errorf("failed to add message %s to batch: %w", record.MessageID, err)
		}
	}
	
	// Commit the batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	
	return nil
}

// GetProcessedRecord retrieves a processed record by message ID
func (p *PebbleIdempotentStorage) GetProcessedRecord(messageID string) (*ProcessedRecord, error) {
	key := []byte(messageID)
	
	value, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil // Return nil for not found, not an error
		}
		return nil, fmt.Errorf("failed to get processed record: %w", err)
	}
	defer closer.Close()
	
	// Deserialize the record from JSON
	var record ProcessedRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal processed record: %w", err)
	}
	
	return &record, nil
}

// CleanupExpired removes expired records from the database
func (p *PebbleIdempotentStorage) CleanupExpired(expiration time.Duration) error {
	cutoffTime := time.Now().Add(-expiration)
	
	// Create iterator to scan all records
	iter := p.db.NewIter(nil)
	defer iter.Close()
	
	batch := p.db.NewBatch()
	defer batch.Close()
	
	deletedCount := 0
	
	for iter.First(); iter.Valid(); iter.Next() {
		// Deserialize the record to check expiration
		var record ProcessedRecord
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			// Skip invalid records
			continue
		}
		
		// Check if record is expired
		if record.ProcessedAt.Before(cutoffTime) {
			if err := batch.Delete(iter.Key(), nil); err != nil {
				return fmt.Errorf("failed to delete expired record: %w", err)
			}
			deletedCount++
		}
		
		// Commit batch in chunks to avoid memory issues
		if deletedCount%1000 == 0 && deletedCount > 0 {
			if err := batch.Commit(pebble.Sync); err != nil {
				return fmt.Errorf("failed to commit cleanup batch: %w", err)
			}
			batch.Close()
			batch = p.db.NewBatch()
		}
	}
	
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during cleanup: %w", err)
	}
	
	// Commit remaining deletions
	if deletedCount%1000 != 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("failed to commit final cleanup batch: %w", err)
		}
	}
	
	return nil
}

// Close closes the PebbleDB database
func (p *PebbleIdempotentStorage) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// GetStats returns database statistics
func (p *PebbleIdempotentStorage) GetStats() (map[string]interface{}, error) {
	metrics := p.db.Metrics()
	
	stats := map[string]interface{}{
		"db_path":           p.dbPath,
		"memtable_size":     metrics.MemTable.Size,
		"cache_size":        metrics.BlockCache.Size,
		"cache_hit_rate":    float64(metrics.BlockCache.Hits) / float64(metrics.BlockCache.Hits+metrics.BlockCache.Misses),
		"compaction_count":  metrics.Compact.Count,
		"flush_count":       metrics.Flush.Count,
	}
	
	return stats, nil
}

// Compact triggers manual compaction of the database
func (p *PebbleIdempotentStorage) Compact() error {
	return p.db.Compact(nil, nil)
}