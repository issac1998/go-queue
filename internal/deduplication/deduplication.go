package deduplication

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"sync"
	"time"
)

// HashType 哈希算法类型
type HashType int8

const (
	MD5 HashType = iota
	SHA256
)

func (h HashType) String() string {
	switch h {
	case MD5:
		return "md5"
	case SHA256:
		return "sha256"
	default:
		return "unknown"
	}
}

// DuplicateEntry duplicate entry information
type DuplicateEntry struct {
	Hash      string    `json:"hash"`
	FirstSeen time.Time `json:"first_seen"`
	Count     int64     `json:"count"`
	LastSeen  time.Time `json:"last_seen"`
	Offset    int64     `json:"offset"` // first occurrence offset
}

// Deduplicator duplicate message deduplicator
type Deduplicator struct {
	hashType   HashType
	entries    map[string]*DuplicateEntry
	mu         sync.RWMutex
	maxEntries int           // maximum number of entries
	ttl        time.Duration // entry TTL
	enabled    bool
	stopCh     chan struct{}
}

// Config duplicate message deduplicator configuration
type Config struct {
	HashType   HashType      `json:"hash_type"`
	MaxEntries int           `json:"max_entries"`
	TTL        time.Duration `json:"ttl"`
	Enabled    bool          `json:"enabled"`
}

// DefaultConfig returns the default deduplicator configuration
func DefaultConfig() *Config {
	return &Config{
		HashType:   SHA256,
		MaxEntries: 100000,         // 100,000 entries
		TTL:        time.Hour * 24, // 24 hours
		Enabled:    true,
	}
}

// NewDeduplicator creates a new Deduplicator
func NewDeduplicator(config *Config) *Deduplicator {
	if config == nil {
		config = DefaultConfig()
	}

	d := &Deduplicator{
		hashType:   config.HashType,
		entries:    make(map[string]*DuplicateEntry),
		maxEntries: config.MaxEntries,
		ttl:        config.TTL,
		enabled:    config.Enabled,
		stopCh:     make(chan struct{}),
	}

	if d.enabled && d.ttl > 0 {
		d.startCleanup()
	}

	return d
}

// IsEnabled checks whether deduplication is enabled
func (d *Deduplicator) IsEnabled() bool {
	return d.enabled
}

// Enable enables deduplication
func (d *Deduplicator) Enable() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enabled = true
	if d.ttl > 0 {
		d.startCleanup()
	}
}

// Disable disables deduplication
func (d *Deduplicator) Disable() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enabled = false
}

// calculateHash calculates the hash of the message
func (d *Deduplicator) calculateHash(data []byte) string {
	var hasher hash.Hash

	switch d.hashType {
	case MD5:
		hasher = md5.New()
	case SHA256:
		hasher = sha256.New()
	default:
		hasher = sha256.New()
	}

	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

// IsDuplicate checks whether the message is a duplicate
// 返回: isDuplicate, originalOffset, error
func (d *Deduplicator) IsDuplicate(data []byte, currentOffset int64) (bool, int64, error) {
	if !d.enabled {
		return false, -1, nil
	}

	hash := d.calculateHash(data)

	d.mu.Lock()
	defer d.mu.Unlock()

	entry, exists := d.entries[hash]
	if exists {
		// 更新统计信息
		entry.Count++
		entry.LastSeen = time.Now()
		return true, entry.Offset, nil
	}

	// if entries exceed max, evict oldest
	if len(d.entries) >= d.maxEntries {
		d.evictOldest()
	}

	// 添加新条目
	now := time.Now()
	d.entries[hash] = &DuplicateEntry{
		Hash:      hash,
		FirstSeen: now,
		Count:     1,
		LastSeen:  now,
		Offset:    currentOffset,
	}

	return false, -1, nil
}

// GetDuplicateInfo 获取重复消息信息
func (d *Deduplicator) GetDuplicateInfo(data []byte) *DuplicateEntry {
	if !d.enabled {
		return nil
	}

	hash := d.calculateHash(data)

	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, exists := d.entries[hash]
	if !exists {
		return nil
	}

	// 返回副本避免并发修改
	return &DuplicateEntry{
		Hash:      entry.Hash,
		FirstSeen: entry.FirstSeen,
		Count:     entry.Count,
		LastSeen:  entry.LastSeen,
		Offset:    entry.Offset,
	}
}

// evictOldest 淘汰最老的条目
func (d *Deduplicator) evictOldest() {
	var oldestHash string
	var oldestTime time.Time

	for hash, entry := range d.entries {
		if oldestHash == "" || entry.FirstSeen.Before(oldestTime) {
			oldestHash = hash
			oldestTime = entry.FirstSeen
		}
	}

	if oldestHash != "" {
		delete(d.entries, oldestHash)
	}
}

// startCleanup starts the cleanup goroutine
func (d *Deduplicator) startCleanup() {
	cleanupInterval := d.ttl / 10 // clean up every 1/10 of TTL
	if cleanupInterval < time.Minute {
		cleanupInterval = time.Minute
	}

	go func() {
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				d.cleanup()
			case <-d.stopCh:
				return
			}
		}
	}()
}

// cleanup cleans up old entries
func (d *Deduplicator) cleanup() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	expiredHashes := make([]string, 0)

	for hash, entry := range d.entries {
		if now.Sub(entry.LastSeen) > d.ttl {
			expiredHashes = append(expiredHashes, hash)
		}
	}

	for _, hash := range expiredHashes {
		delete(d.entries, hash)
	}
}

// GetStats returns the current deduplicator statistics
func (d *Deduplicator) GetStats() *Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	totalCount := int64(0)
	totalDuplicates := int64(0)

	for _, entry := range d.entries {
		totalCount += entry.Count
		if entry.Count > 1 {
			totalDuplicates += entry.Count - 1 // 减去第一次出现
		}
	}

	return &Stats{
		UniqueMessages:    int64(len(d.entries)),
		TotalMessages:     totalCount,
		DuplicateMessages: totalDuplicates,
		DuplicateRate:     float64(totalDuplicates) / float64(totalCount),
		HashType:          d.hashType.String(),
		Enabled:           d.enabled,
	}
}

// Stats duplicate message deduplication statistics
type Stats struct {
	UniqueMessages    int64   `json:"unique_messages"`
	TotalMessages     int64   `json:"total_messages"`
	DuplicateMessages int64   `json:"duplicate_messages"`
	DuplicateRate     float64 `json:"duplicate_rate"`
	HashType          string  `json:"hash_type"`
	Enabled           bool    `json:"enabled"`
}

// Clear 1
func (d *Deduplicator) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = make(map[string]*DuplicateEntry)
}

// Close
func (d *Deduplicator) Close() {
	close(d.stopCh)
}

// Todo: Add persistence of deduplication entries to disk for recovery after restart
// CalculateMessageHash calculate the hash of the message using the specified hash type
func CalculateMessageHash(data []byte, hashType HashType) string {
	var hasher hash.Hash

	switch hashType {
	case MD5:
		hasher = md5.New()
	case SHA256:
		hasher = sha256.New()
	default:
		hasher = sha256.New()
	}

	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}
