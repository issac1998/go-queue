package metadata

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/compression"
	"github.com/issac1998/go-queue/internal/deduplication"
	"github.com/issac1998/go-queue/internal/storage"
)

// Manager defines the manager of the message queue system that coordinates
// all operations including topic management, message storage, and consumer groups
type Manager struct {
	Config    *Config
	IsRunning bool

	Topics map[string]*Topic

	mu sync.RWMutex

	ctx           context.Context
	cancel        context.CancelFunc
	cleanupTicker *time.Ticker
	flushTicker   *time.Ticker

	Stats *SystemStats

	Metrics *Metrics

	Compressor           compression.Compressor
	Deduplicator         *deduplication.Deduplicator
	CompressionEnabled   bool
	DeduplicationEnabled bool

	ConsumerGroups *ConsumerGroupManager
}

// TopicConfig contains configuration parameters for creating a topic
type TopicConfig struct {
	Partitions int32
	// Replicas is the number of replicas per partition (for future cluster support)
	Replicas int32
}

// Config contains all system configuration parameters
type Config struct {
	DataDir            string        `json:"data_dir"`
	MaxTopicPartitions int           `json:"max_topic_partitions"`
	SegmentSize        int64         `json:"segment_size"`
	RetentionTime      time.Duration `json:"retention_time"`
	MaxStorageSize     int64         `json:"max_storage_size"`

	FlushInterval   time.Duration `json:"flush_interval"`
	CleanupInterval time.Duration `json:"cleanup_interval"`
	MaxMessageSize  int           `json:"max_message_size"`

	// Compression configuration
	CompressionEnabled   bool                        `json:"compression_enabled"`
	CompressionType      compression.CompressionType `json:"compression_type"`
	CompressionThreshold int                         `json:"compression_threshold"` // Compress only messages above this byte count

	// Deduplication configuration
	DeduplicationEnabled bool                  `json:"deduplication_enabled"`
	DeduplicationConfig  *deduplication.Config `json:"deduplication_config"`
}

// SystemStats contains system-wide statistics
type SystemStats struct {
	mu sync.RWMutex

	// Basic statistics
	TotalTopics     int64 `json:"total_topics"`
	TotalPartitions int64 `json:"total_partitions"`
	TotalSegments   int64 `json:"total_segments"`
	TotalMessages   int64 `json:"total_messages"`
	TotalBytes      int64 `json:"total_bytes"`

	// Performance statistics
	MessagesPerSecond float64 `json:"messages_per_second"`
	BytesPerSecond    float64 `json:"bytes_per_second"`
	AvgLatency        float64 `json:"avg_latency"`

	// Time statistics
	StartTime      time.Time     `json:"start_time"`
	LastUpdateTime time.Time     `json:"last_update_time"`
	Uptime         time.Duration `json:"uptime"`
}

// Metrics contains monitoring and performance metrics
type Metrics struct {
	mu sync.RWMutex

	// Request statistics
	RequestsTotal   int64 `json:"requests_total"`
	RequestsSuccess int64 `json:"requests_success"`
	RequestsFailed  int64 `json:"requests_failed"`

	// Error statistics
	ErrorsTotal  int64            `json:"errors_total"`
	ErrorsByType map[string]int64 `json:"errors_by_type"`

	// Resource usage
	MemoryUsage int64   `json:"memory_usage"`
	DiskUsage   int64   `json:"disk_usage"`
	CPUUsage    float64 `json:"cpu_usage"`
}

// TopicInfo contains detailed information about a topic
type TopicInfo struct {
	Name             string
	Partitions       int32
	Replicas         int32
	CreatedAt        time.Time
	Size             int64
	MessageCount     int64
	PartitionDetails []PartitionInfo
}

// PartitionInfo contains detailed information about a partition
type PartitionInfo struct {
	ID           int32
	Leader       int32
	Replicas     []int32
	ISR          []int32
	Size         int64
	MessageCount int64
	StartOffset  int64
	EndOffset    int64
}

// SimpleTopicInfo contains basic information about a topic
type SimpleTopicInfo struct {
	TopicName    string
	Partitions   int32
	MessageCount int64
	Size         int64
}

// NewManager creates a new Manager instance with the given configuration
func NewManager(config *Config) (*Manager, error) {
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}

	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data directory failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var compressor compression.Compressor
	if config.CompressionEnabled {
		var err error
		compressor, err = compression.GetCompressor(config.CompressionType)
		if err != nil {
			return nil, fmt.Errorf("create compressor failed: %v", err)
		}
	} else {
		compressor, _ = compression.GetCompressor(compression.None)
	}

	var deduplicator *deduplication.Deduplicator
	if config.DeduplicationEnabled {
		if config.DeduplicationConfig == nil {
			config.DeduplicationConfig = deduplication.DefaultConfig()
		}
		deduplicator = deduplication.NewDeduplicator(config.DeduplicationConfig)
	}

	manager := &Manager{
		Config: config,
		Topics: make(map[string]*Topic),
		ctx:    ctx,
		cancel: cancel,
		Stats:  &SystemStats{},
		Metrics: &Metrics{
			ErrorsByType: make(map[string]int64),
		},

		Compressor:           compressor,
		Deduplicator:         deduplicator,
		CompressionEnabled:   config.CompressionEnabled,
		DeduplicationEnabled: config.DeduplicationEnabled,

		ConsumerGroups: NewConsumerGroupManager(),
	}

	manager.Stats.StartTime = time.Now()
	manager.Stats.LastUpdateTime = time.Now()

	return manager, nil
}

// Start initializes and starts the manager, loading existing data and starting background tasks
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.loadExistingData(); err != nil {
		return fmt.Errorf("load existing data failed: %v", err)
	}

	m.startBackgroundTasks()

	return nil
}

// Stop gracefully shuts down the manager, stopping background tasks and closing resources
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopBackgroundTasks()

	if err := m.closeAllResources(); err != nil {
		return fmt.Errorf("close resources failed: %v", err)
	}

	log.Printf("Manager stopped successfully")
	return nil
}

// CreateTopic creates a new topic with the specified name and configuration
func (m *Manager) CreateTopic(name string, config *TopicConfig) (*Topic, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if int(config.Partitions) > m.Config.MaxTopicPartitions {
		return nil, fmt.Errorf("partitions %d exceeds max allowed %d", config.Partitions, m.Config.MaxTopicPartitions)
	}

	if _, exists := m.Topics[name]; exists {
		return nil, fmt.Errorf("topic %s already exists", name)
	}

	topic, err := NewTopic(name, config, m.Config)
	if err != nil {
		return nil, fmt.Errorf("create topic failed: %v", err)
	}

	m.Topics[name] = topic
	m.Stats.TotalTopics++

	m.updateStats()

	log.Printf("Topic %s created successfully", name)
	return topic, nil
}

// NewTopic creates a new Topic instance with the given parameters
func NewTopic(name string, config *TopicConfig, sysConfig *Config) (*Topic, error) {
	topic := &Topic{
		Name:       name,
		Config:     config,
		Partitions: make(map[int32]*Partition),
	}

	for i := int32(0); i < config.Partitions; i++ {
		partition, err := NewPartition(i, name, sysConfig)
		if err != nil {
			return nil, err
		}
		topic.Partitions[i] = partition
	}
	return topic, nil
}

// GetPartition returns a specific partition for a topic
func (t *Topic) GetPartition(id int32) (*Partition, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p, ok := t.Partitions[id]
	if !ok {
		return nil, fmt.Errorf("partition %d not found", id)
	}
	return p, nil
}

// Close closes all partitions in the topic
func (t *Topic) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.Partitions {
		if err := p.Close(); err != nil {
			return err
		}
	}
	return nil
}

// GetTopic retrieves a topic by name
func (m *Manager) GetTopic(name string) (*Topic, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topic, exists := m.Topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	return topic, nil
}

// DeleteTopic removes a topic and all its associated data
func (m *Manager) DeleteTopic(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	topic, exists := m.Topics[name]
	if !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	if err := topic.Close(); err != nil {
		log.Printf("Warning: failed to close topic %s properly: %v", name, err)
	}

	topicDataDir := filepath.Join(m.Config.DataDir, name)
	if err := os.RemoveAll(topicDataDir); err != nil {
		log.Printf("Warning: failed to remove topic data directory %s: %v", topicDataDir, err)
	}

	delete(m.Topics, name)
	m.Stats.TotalTopics--

	m.updateStats()

	log.Printf("Topic %s deleted successfully", name)
	return nil
}

// ListTopics returns a list of all topic names
func (m *Manager) ListTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topics := make([]string, 0, len(m.Topics))
	for name := range m.Topics {
		topics = append(topics, name)
	}

	return topics
}

// ListTopicsDetailed returns detailed information about all topics
func (m *Manager) ListTopicsDetailed() ([]TopicInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var topicInfos []TopicInfo

	for _, topic := range m.Topics {
		topic.mu.RLock()

		topicInfo := TopicInfo{
			Name:             topic.Name,
			Partitions:       topic.Config.Partitions,
			Replicas:         topic.Config.Replicas,
			CreatedAt:        time.Now(),
			Size:             0,
			MessageCount:     0,
			PartitionDetails: make([]PartitionInfo, 0, len(topic.Partitions)),
		}

		for partitionID, partition := range topic.Partitions {
			partition.Mu.RLock()

			partitionSize := int64(0)
			partitionMsgCount := int64(0)
			startOffset := int64(0)
			endOffset := int64(0)

			for _, segment := range partition.Segments {
				if segment != nil {
					partitionSize += segment.CurrentSize
					partitionMsgCount += segment.WriteCount
					if endOffset < segment.BaseOffset+segment.WriteCount {
						endOffset = segment.BaseOffset + segment.WriteCount
					}
				}
			}

			partitionInfo := PartitionInfo{
				ID:           partitionID,
				Leader:       0,
				Replicas:     []int32{0},
				ISR:          []int32{0},
				Size:         partitionSize,
				MessageCount: partitionMsgCount,
				StartOffset:  startOffset,
				EndOffset:    endOffset,
			}

			topicInfo.PartitionDetails = append(topicInfo.PartitionDetails, partitionInfo)
			topicInfo.Size += partitionSize
			topicInfo.MessageCount += partitionMsgCount

			partition.Mu.RUnlock()
		}

		topicInfos = append(topicInfos, topicInfo)
		topic.mu.RUnlock()
	}

	return topicInfos, nil
}

// GetPartition retrieves a specific partition from a topic
func (m *Manager) GetPartition(topicName string, partitionID int32) (*Partition, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topic, exists := m.Topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	partition, err := topic.GetPartition(partitionID)
	if err != nil {
		return nil, fmt.Errorf("get partition failed: %v", err)
	}

	return partition, nil
}

// WriteMessage writes a message to the specified topic and partition
func (m *Manager) WriteMessage(topicName string, partitionID int32, message []byte) (int64, error) {
	if len(message) > m.Config.MaxMessageSize {
		return 0, fmt.Errorf("message too large: %d bytes", len(message))
	}

	if m.DeduplicationEnabled && m.Deduplicator != nil {
		partition, err := m.GetPartition(topicName, partitionID)
		if err != nil {
			return 0, err
		}

		var nextOffset int64
		if partition.ActiveSeg != nil {
			nextOffset = partition.ActiveSeg.BaseOffset + partition.ActiveSeg.WriteCount
		}

		isDupe, originalOffset, err := m.Deduplicator.IsDuplicate(message, nextOffset)
		if err != nil {
			log.Printf("Deduplication check failed: %v", err)
		} else if isDupe {
			log.Printf("Duplicate message found, returning original offset: %d", originalOffset)
			return originalOffset, nil
		}
	}

	processedMessage := message
	if m.CompressionEnabled && len(message) >= m.Config.CompressionThreshold {
		compressed, err := compression.CompressMessage(message, m.Config.CompressionType)
		if err != nil {
			log.Printf("Message compression failed: %v", err)
		} else {
			if len(compressed) < len(message)*8/10 {
				processedMessage = compressed
				log.Printf("Message compressed: %d -> %d bytes (ratio: %.2f%%)",
					len(message), len(compressed),
					float64(len(compressed))/float64(len(message))*100)
			}
		}
	}

	partition, err := m.GetPartition(topicName, partitionID)
	if err != nil {
		return 0, err
	}

	offset, err := partition.Append(processedMessage)
	if err != nil {
		m.recordError("write_failed")
		return 0, fmt.Errorf("write message failed: %v", err)
	}

	m.recordSuccess()
	m.updateStats()

	return offset, nil
}

// ReadMessage reads messages from the specified topic, partition and offset
func (m *Manager) ReadMessage(topicName string, partitionID int32, offset int64, maxBytes int32) ([][]byte, int64, error) {
	partition, err := m.GetPartition(topicName, partitionID)
	if err != nil {
		return nil, 0, err
	}

	rawMessages, nextOffset, err := partition.Read(offset, maxBytes)
	if err != nil {
		return nil, 0, err
	}

	if m.CompressionEnabled {
		decompressedMessages := make([][]byte, 0, len(rawMessages))
		for _, rawMsg := range rawMessages {

			if len(rawMsg) >= 5 {
				decompressed, err := compression.DecompressMessage(rawMsg)
				if err != nil {

					decompressedMessages = append(decompressedMessages, rawMsg)
				} else {
					decompressedMessages = append(decompressedMessages, decompressed)
				}
			} else {

				decompressedMessages = append(decompressedMessages, rawMsg)
			}
		}
		return decompressedMessages, nextOffset, nil
	}

	return rawMessages, nextOffset, nil
}

// GetStats returns the current system statistics
func (m *Manager) GetStats() *SystemStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.Stats.Uptime = time.Since(m.Stats.StartTime)
	m.Stats.LastUpdateTime = time.Now()

	return m.Stats
}

// GetMetrics returns the current system metrics
func (m *Manager) GetMetrics() *Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.Metrics
}

// DescribeTopic returns detailed information about a specific topic
func (m *Manager) GetTopicInfo(topicName string) (*TopicInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topic, exists := m.Topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not found", topicName)
	}

	topic.mu.RLock()
	defer topic.mu.RUnlock()

	topicInfo := &TopicInfo{
		Name:             topic.Name,
		Partitions:       topic.Config.Partitions,
		Replicas:         topic.Config.Replicas,
		CreatedAt:        time.Now(),
		Size:             0,
		MessageCount:     0,
		PartitionDetails: make([]PartitionInfo, 0, len(topic.Partitions)),
	}

	for partitionID, partition := range topic.Partitions {
		partition.Mu.RLock()

		partitionSize := int64(0)
		partitionMsgCount := int64(0)
		startOffset := int64(0)
		endOffset := int64(0)

		for _, segment := range partition.Segments {
			if segment != nil {
				partitionSize += segment.CurrentSize
				partitionMsgCount += segment.WriteCount
				if endOffset < segment.BaseOffset+segment.WriteCount {
					endOffset = segment.BaseOffset + segment.WriteCount
				}
			}
		}

		partitionInfo := PartitionInfo{
			ID:           partitionID,
			Leader:       0,
			Replicas:     []int32{0},
			ISR:          []int32{0},
			Size:         partitionSize,
			MessageCount: partitionMsgCount,
			StartOffset:  startOffset,
			EndOffset:    endOffset,
		}

		topicInfo.PartitionDetails = append(topicInfo.PartitionDetails, partitionInfo)
		topicInfo.Size += partitionSize
		topicInfo.MessageCount += partitionMsgCount

		partition.Mu.RUnlock()
	}

	return topicInfo, nil
}


func (m *Manager) startBackgroundTasks() {
	go m.flushTask()

	go m.cleanupTask()

	go m.statsUpdateTask()

	go m.consumerGroupCleanupTask()

}

func (m *Manager) stopBackgroundTasks() {
	if m.cleanupTicker != nil {
		m.cleanupTicker.Stop()
	}
	if m.flushTicker != nil {
		m.flushTicker.Stop()
	}
	m.cancel()
}

func (m *Manager) cleanupTask() {
	m.cleanupTicker = time.NewTicker(m.Config.CleanupInterval)

	for {
		select {
		case <-m.cleanupTicker.C:
			if err := m.cleanupExpiredMessages(); err != nil {
				log.Printf("Cleanup failed: %v", err)
			}
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) statsUpdateTask() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateStats()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) loadExistingData() error {
	log.Printf("Loading existing data from %s", m.Config.DataDir)

	if _, err := os.Stat(m.Config.DataDir); os.IsNotExist(err) {
		log.Printf("Data directory %s does not exist, creating...", m.Config.DataDir)
		if err := os.MkdirAll(m.Config.DataDir, 0755); err != nil {
			return fmt.Errorf("create data directory failed: %v", err)
		}
		return nil
	}

	entries, err := os.ReadDir(m.Config.DataDir)
	if err != nil {
		return fmt.Errorf("read data directory failed: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		topicName := entry.Name()
		topicPath := filepath.Join(m.Config.DataDir, topicName)

		if err := m.loadTopic(topicName, topicPath); err != nil {
			log.Printf("Failed to load topic %s: %v", topicName, err)
			continue
		}
	}

	log.Printf("Loaded %d topics", len(m.Topics))
	return nil
}

// loadTopic
func (m *Manager) loadTopic(topicName, topicPath string) error {
	entries, err := os.ReadDir(topicPath)
	if err != nil {
		return fmt.Errorf("read topic directory failed: %v", err)
	}

	topic := &Topic{
		Name:       topicName,
		Partitions: make(map[int32]*Partition),
		Config:     &TopicConfig{},
	}

	var partitionCount int32
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		if !strings.HasPrefix(entry.Name(), "partition-") {
			continue
		}

		partitionIDStr := strings.TrimPrefix(entry.Name(), "partition-")
		partitionID, err := strconv.ParseInt(partitionIDStr, 10, 32)
		if err != nil {
			log.Printf("Invalid partition directory name: %s", entry.Name())
			continue
		}

		partition, err := m.loadPartition(int32(partitionID), topicName, filepath.Join(topicPath, entry.Name()))
		if err != nil {
			log.Printf("Failed to load partition %d: %v", partitionID, err)
			continue
		}

		topic.Partitions[int32(partitionID)] = partition
		partitionCount++
	}

	if partitionCount > 0 {
		topic.Config.Partitions = partitionCount
		m.Topics[topicName] = topic
		log.Printf("Loaded topic %s with %d partitions", topicName, partitionCount)
	}

	return nil
}

func (m *Manager) loadPartition(partitionID int32, topicName, partitionPath string) (*Partition, error) {
	entries, err := os.ReadDir(partitionPath)
	if err != nil {
		return nil, fmt.Errorf("read partition directory failed: %v", err)
	}

	var segmentFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			segmentFiles = append(segmentFiles, entry.Name())
		}
	}

	sort.Strings(segmentFiles)

	partition := &Partition{
		ID:         partitionID,
		Topic:      topicName,
		DataDir:    partitionPath,
		Segments:   make(map[int]*storage.Segment),
		MaxSegSize: m.Config.SegmentSize,
		Mu:         sync.RWMutex{},
	}

	for i, segmentFile := range segmentFiles {
		baseOffsetStr := strings.TrimSuffix(segmentFile, ".log")
		baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
		if err != nil {
			log.Printf("Invalid segment file name: %s", segmentFile)
			continue
		}

		segment, err := storage.NewSegment(partitionPath, baseOffset, m.Config.SegmentSize)
		if err != nil {
			log.Printf("Failed to load segment %s: %v", segmentFile, err)
			continue
		}

		partition.Segments[i] = segment

		if i == len(segmentFiles)-1 {
			partition.ActiveSeg = segment
		}
	}

	if len(partition.Segments) == 0 {
		segment, err := storage.NewSegment(partitionPath, 0, m.Config.SegmentSize)
		if err != nil {
			return nil, fmt.Errorf("create initial segment failed: %v", err)
		}
		partition.Segments[0] = segment
		partition.ActiveSeg = segment
	}

	log.Printf("Loaded partition %d with %d segments", partitionID, len(partition.Segments))
	return partition, nil
}

func (m *Manager) closeAllResources() error {
	for _, topic := range m.Topics {
		if err := topic.Close(); err != nil {
			log.Printf("Close topic failed: %v", err)
		}
	}

	return nil
}

func (m *Manager) cleanupExpiredMessages() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	//rentention
	expireBefore := now.Add(-m.Config.RetentionTime)

	for _, topic := range m.Topics {
		for _, partition := range topic.Partitions {
			partition.Mu.Lock()
			for segID, segment := range partition.Segments {
				if segment.MaxTimestamp.Before(expireBefore) {
					//delete all segment
					segment.Close() // 先关闭
					delete(partition.Segments, segID)
				} else if segment.MinTimestamp.Before(expireBefore) {

					segment.PurgeBefore(expireBefore)
				}
			}
			partition.Mu.Unlock()
		}
	}
	return nil
}

func (m *Manager) updateStats() {
	m.Stats.mu.Lock()
	defer m.Stats.mu.Unlock()

	totalMessages := int64(0)
	totalBytes := int64(0)
	totalPartitions := int64(0)
	totalSegments := int64(0)

	for _, topic := range m.Topics {
		for _, partition := range topic.Partitions {
			totalPartitions++
			for _, segment := range partition.Segments {
				totalSegments++
				totalBytes += segment.CurrentSize
			}
		}
	}

	m.Stats.TotalMessages = totalMessages
	m.Stats.TotalBytes = totalBytes
	m.Stats.TotalPartitions = totalPartitions
	m.Stats.TotalSegments = totalSegments
}

func (m *Manager) updateMetrics() {
	m.Metrics.mu.Lock()
	defer m.Metrics.mu.Unlock()

	var totalMemoryUsage int64
	var totalDiskUsage int64

	for _, topic := range m.Topics {
		for _, partition := range topic.Partitions {
			for _, segment := range partition.Segments {
				totalMemoryUsage += int64(len(segment.IndexEntries) * 24)
				totalDiskUsage += segment.CurrentSize
			}
		}
	}

	m.Metrics.MemoryUsage = totalMemoryUsage
	m.Metrics.DiskUsage = totalDiskUsage

	// TODO: cpu usage
	m.Metrics.CPUUsage = 0.0
}

func (m *Manager) recordSuccess() {
	m.Metrics.mu.Lock()
	defer m.Metrics.mu.Unlock()

	m.Metrics.RequestsTotal++
	m.Metrics.RequestsSuccess++
}

func (m *Manager) recordError(errorType string) {
	m.Metrics.mu.Lock()
	defer m.Metrics.mu.Unlock()

	m.Metrics.RequestsTotal++
	m.Metrics.RequestsFailed++
	m.Metrics.ErrorsTotal++
	m.Metrics.ErrorsByType[errorType]++
}

func validateConfig(config *Config) error {
	if config.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	if config.SegmentSize <= 0 {
		return fmt.Errorf("segment size must be positive")
	}
	if config.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be positive")
	}
	return nil
}

func (m *Manager) flushTask() {
	m.flushTicker = time.NewTicker(m.Config.FlushInterval)

	for {
		select {
		case <-m.flushTicker.C:
			m.flushAll()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) flushAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, topic := range m.Topics {
		for _, partition := range topic.Partitions {
			partition.Flush()
		}
	}
}

// Flush sync all segments
func (p *Partition) Flush() {
	p.Mu.RLock()
	defer p.Mu.RUnlock()
	for _, segment := range p.Segments {
		segment.Sync()
	}
}

func (m *Manager) consumerGroupCleanupTask() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.ConsumerGroups.CleanupExpiredConsumers()
		case <-m.ctx.Done():
			return
		}
	}
}
