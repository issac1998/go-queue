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

// DuplicateEntry 重复条目
type DuplicateEntry struct {
	Hash      string    `json:"hash"`
	FirstSeen time.Time `json:"first_seen"`
	Count     int64     `json:"count"`
	LastSeen  time.Time `json:"last_seen"`
	Offset    int64     `json:"offset"` // 第一次出现的offset
}

// Deduplicator 去重器
type Deduplicator struct {
	hashType    HashType
	entries     map[string]*DuplicateEntry
	mu          sync.RWMutex
	maxEntries  int           // 最大条目数
	ttl         time.Duration // 条目存活时间
	enabled     bool
	cleanupTick *time.Ticker
	stopCh      chan struct{}
}

// Config 去重器配置
type Config struct {
	HashType   HashType      `json:"hash_type"`
	MaxEntries int           `json:"max_entries"`
	TTL        time.Duration `json:"ttl"`
	Enabled    bool          `json:"enabled"`
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		HashType:   SHA256,
		MaxEntries: 100000,         // 10万条目
		TTL:        time.Hour * 24, // 24小时
		Enabled:    true,
	}
}

// NewDeduplicator 创建新的去重器
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

// IsEnabled 检查是否启用去重
func (d *Deduplicator) IsEnabled() bool {
	return d.enabled
}

// Enable 启用去重
func (d *Deduplicator) Enable() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enabled = true
	if d.cleanupTick == nil && d.ttl > 0 {
		d.startCleanup()
	}
}

// Disable 禁用去重
func (d *Deduplicator) Disable() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.enabled = false
	if d.cleanupTick != nil {
		d.cleanupTick.Stop()
		d.cleanupTick = nil
	}
}

// calculateHash 计算消息哈希
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

// IsDuplicate 检查消息是否重复
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

	// 检查是否达到最大条目数
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

// startCleanup 启动清理goroutine
func (d *Deduplicator) startCleanup() {
	cleanupInterval := d.ttl / 10 // 每1/10 TTL时间清理一次
	if cleanupInterval < time.Minute {
		cleanupInterval = time.Minute
	}

	d.cleanupTick = time.NewTicker(cleanupInterval)

	go func() {
		for {
			select {
			case <-d.cleanupTick.C:
				d.cleanup()
			case <-d.stopCh:
				return
			}
		}
	}()
}

// cleanup 清理过期条目
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

// GetStats 获取去重统计信息
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

// Stats 去重统计信息
type Stats struct {
	UniqueMessages    int64   `json:"unique_messages"`
	TotalMessages     int64   `json:"total_messages"`
	DuplicateMessages int64   `json:"duplicate_messages"`
	DuplicateRate     float64 `json:"duplicate_rate"`
	HashType          string  `json:"hash_type"`
	Enabled           bool    `json:"enabled"`
}

// Clear 清空所有去重条目
func (d *Deduplicator) Clear() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = make(map[string]*DuplicateEntry)
}

// Close 关闭去重器
func (d *Deduplicator) Close() {
	if d.cleanupTick != nil {
		d.cleanupTick.Stop()
	}
	close(d.stopCh)
}

// CalculateMessageHash 计算消息哈希（公共函数）
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
