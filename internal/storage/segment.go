package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	IndexEntrySize = 16   // 8 bytes offset + 8 bytes position
	IndexInterval  = 1024 // 每1KB写入一个索引条目
)

// Segment 表示一个日志段，包含消息数据和索引
type Segment struct {
	// --- 基础元数据 ---
	BaseOffset int64 // 当前 Segment 的起始 Offset
	EndOffset  int64 // 当前 Segment 的结束 Offset
	MaxBytes   int64 // Segment 的最大容量
	IsActive   bool  // 是否为活跃 Segment

	// --- 文件句柄 ---
	LogFile       *os.File // 日志文件
	IndexFile     *os.File // 索引文件
	TimeIndexFile *os.File // 时间索引文件

	DataDir string // 数据目录

	// --- 并发控制 ---
	Mu sync.RWMutex

	// --- 内存缓存 ---
	IndexEntries []IndexEntry // 内存中的索引条目
	CurrentSize  int64        // 当前日志文件大小
	LastSynced   int64        // 最后持久化的位置

	// --- 统计信息 ---
	WriteCount    int64     // 写入次数
	ReadCount     int64     // 读取次数
	LastWriteTime time.Time // 最后写入时间
	LastReadTime  time.Time // 最后读取时间
}

// IndexEntry 索引条目
type IndexEntry struct {
	Offset   int64 // 消息Offset
	Position int64 // 在日志文件中的位置
	TimeMs   int64 // 消息时间戳（毫秒）
}

// Append
func NewSegment(dir string, baseOffset int64, maxBytes int64) (*Segment, error) {
	// 创建目录（如果不存在）
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory failed: %v", err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	timeIndexPath := filepath.Join(dir, fmt.Sprintf("%020d.timeindex", baseOffset))

	// 打开日志文件
	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file failed: %v", err)
	}

	// 打开索引文件
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("open index file failed: %v", err)
	}

	// 打开时间索引文件
	timeIndexFile, err := os.OpenFile(timeIndexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("open time index file failed: %v", err)
	}

	// 获取当前日志文件大小
	stat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		indexFile.Close()
		timeIndexFile.Close()
		return nil, fmt.Errorf("get log file size failed: %v", err)
	}

	segment := &Segment{
		BaseOffset:    baseOffset,
		MaxBytes:      maxBytes,
		IsActive:      true,
		LogFile:       logFile,
		IndexFile:     indexFile,
		TimeIndexFile: timeIndexFile,
		DataDir:       dir,
		CurrentSize:   stat.Size(),
		IndexEntries:  make([]IndexEntry, 0),
		LastWriteTime: time.Now(),
		LastReadTime:  time.Now(),
	}

	// 加载现有索引
	if err := segment.loadIndex(); err != nil {
		segment.Close()
		return nil, fmt.Errorf("load index failed: %v", err)
	}

	return segment, nil

}

// loadIndex 从索引文件加载索引条目到内存
func (s *Segment) loadIndex() error {
	stat, err := s.IndexFile.Stat()
	if err != nil {
		return err
	}

	// 计算索引条目数量
	numEntries := stat.Size() / IndexEntrySize
	s.IndexEntries = make([]IndexEntry, 0, numEntries)

	// 读取所有索引条目
	for i := int64(0); i < numEntries; i++ {
		var entry IndexEntry
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Offset); err != nil {
			return err
		}
		if err := binary.Read(s.IndexFile, binary.BigEndian, &entry.Position); err != nil {
			return err
		}
		s.IndexEntries = append(s.IndexEntries, entry)
	}

	return nil
}

// Append 追加消息到日志段
func (s *Segment) Append(msg []byte) (offset int64, err error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	// 检查是否超过最大容量
	if s.CurrentSize+int64(len(msg)+4) > s.MaxBytes {
		return 0, errors.New("segment is full")
	}

	// 计算消息的offset
	offset = s.BaseOffset + s.CurrentSize

	// 写入消息长度和内容
	if err := binary.Write(s.LogFile, binary.BigEndian, int32(len(msg))); err != nil {
		return 0, fmt.Errorf("write message length failed: %v", err)
	}
	if _, err := s.LogFile.Write(msg); err != nil {
		return 0, fmt.Errorf("write message content failed: %v", err)
	}

	// 更新当前大小
	s.CurrentSize += int64(len(msg) + 4)
	s.WriteCount++

	// 每隔一定间隔写入索引条目
	if s.CurrentSize%IndexInterval == 0 {
		pos, err := s.LogFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("get current position failed: %v", err)
		}

		// 写入索引条目
		if err := binary.Write(s.IndexFile, binary.BigEndian, offset); err != nil {
			return 0, fmt.Errorf("write index offset failed: %v", err)
		}
		if err := binary.Write(s.IndexFile, binary.BigEndian, pos); err != nil {
			return 0, fmt.Errorf("write index position failed: %v", err)
		}

		// 添加到内存索引
		s.IndexEntries = append(s.IndexEntries, IndexEntry{
			Offset:   offset,
			Position: pos,
		})
	}

	return offset, nil
}

// FindPosition 通过Offset查找文件位置
func (s *Segment) FindPosition(offset int64) (int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	// 检查offset是否在范围内
	if offset < s.BaseOffset || offset >= s.BaseOffset+s.CurrentSize {
		return 0, errors.New("offset out of range")
	}

	// 二分查找最近的索引条目
	left := 0
	right := len(s.IndexEntries) - 1
	var nearestEntry IndexEntry

	for left <= right {
		mid := left + (right-left)/2
		if s.IndexEntries[mid].Offset == offset {
			return s.IndexEntries[mid].Position, nil
		} else if s.IndexEntries[mid].Offset < offset {
			nearestEntry = s.IndexEntries[mid]
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	// 如果没有找到精确匹配，从最近的索引条目开始扫描
	if nearestEntry.Offset == 0 {
		return s.BaseOffset, nil
	}

	return nearestEntry.Position, nil
}

// ReadAt 从指定位置读取数据
func (s *Segment) ReadAt(pos int64, buf []byte) (int, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	// 检查位置是否有效
	if pos < 0 || pos >= s.CurrentSize {
		return 0, errors.New("invalid position")
	}

	// 定位到指定位置
	if _, err := s.LogFile.Seek(pos, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek failed: %v", err)
	}

	// 读取数据
	n, err := io.ReadFull(s.LogFile, buf)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("read failed: %v", err)
	}

	return n, nil
}

// Close 关闭Segment
func (s *Segment) Close() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	var errs []error

	// 关闭所有文件
	if s.LogFile != nil {
		if err := s.LogFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close log file failed: %v", err))
		}
	}
	if s.IndexFile != nil {
		if err := s.IndexFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close index file failed: %v", err))
		}
	}
	if s.TimeIndexFile != nil {
		if err := s.TimeIndexFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close time index file failed: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close segment failed: %v", errs)
	}
	return nil
}

// Sync 将数据同步到磁盘
func (s *Segment) Sync() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if err := s.LogFile.Sync(); err != nil {
		return fmt.Errorf("sync log file failed: %v", err)
	}
	if err := s.IndexFile.Sync(); err != nil {
		return fmt.Errorf("sync index file failed: %v", err)
	}
	s.LastSynced = s.CurrentSize
	return nil
}
