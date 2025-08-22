package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewSegment(t *testing.T) {
	// Create temporary directory for test
	tempDir := t.TempDir()

	segment, err := NewSegment(tempDir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer segment.Close()

	if segment.BaseOffset != 0 {
		t.Errorf("expected BaseOffset 0, got %d", segment.BaseOffset)
	}
	if segment.MaxBytes != 1024*1024 {
		t.Errorf("expected MaxBytes 1048576, got %d", segment.MaxBytes)
	}
	if !segment.IsActive {
		t.Error("expected segment to be active")
	}
}

func TestSegmentAppend(t *testing.T) {
	tempDir := t.TempDir()
	segment, err := NewSegment(tempDir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer segment.Close()

	// Test appending messages
	messages := []string{
		"Hello, World!",
		"This is a test message",
		"Another message",
	}

	var offsets []int64
	for i, msg := range messages {
		offset, err := segment.Append([]byte(msg), time.Now())
		if err != nil {
			t.Fatalf("failed to append message %d: %v", i, err)
		}
		offsets = append(offsets, offset)
	}

	// Verify offsets are sequential
	for i, offset := range offsets {
		expectedOffset := int64(i)
		if offset != expectedOffset {
			t.Errorf("expected offset %d, got %d", expectedOffset, offset)
		}
	}

	// Verify write count
	if segment.WriteCount != int64(len(messages)) {
		t.Errorf("expected WriteCount %d, got %d", len(messages), segment.WriteCount)
	}
}

func TestSegmentReadAt(t *testing.T) {
	tempDir := t.TempDir()
	segment, err := NewSegment(tempDir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer segment.Close()

	// Append test messages
	testMessages := []string{
		"Message 1",
		"Message 2",
		"Message 3",
	}

	for _, msg := range testMessages {
		_, err := segment.Append([]byte(msg), time.Now())
		if err != nil {
			t.Fatalf("failed to append message: %v", err)
		}
	}

	// Test reading messages via ReadAt and FindPosition
	// Verify we can find positions for the messages
	for i := range testMessages {
		position, err := segment.FindPosition(int64(i))
		if err != nil {
			t.Errorf("failed to find position for offset %d: %v", i, err)
		}
		if position < 0 {
			t.Errorf("expected positive position for offset %d, got %d", i, position)
		}

		// Test ReadAt functionality
		buf := make([]byte, 100)
		n, err := segment.ReadAt(position, buf)
		if err != nil {
			t.Errorf("failed to read at position %d: %v", position, err)
		}
		if n <= 4 { // Should read at least message length + some content
			t.Errorf("expected to read more than 4 bytes, got %d bytes", n)
		}
	}
}

func TestSegmentFindPosition(t *testing.T) {
	tempDir := t.TempDir()
	segment, err := NewSegment(tempDir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer segment.Close()

	// Append a few messages
	for i := 0; i < 3; i++ {
		_, err := segment.Append([]byte("test message"), time.Now())
		if err != nil {
			t.Fatalf("failed to append message: %v", err)
		}
	}

	// Test finding position for existing offset
	position, err := segment.FindPosition(1)
	if err != nil {
		t.Fatalf("failed to find position for offset 1: %v", err)
	}

	if position < 0 {
		t.Errorf("expected positive position, got %d", position)
	}

	// Test finding position for non-existent offset
	_, err = segment.FindPosition(10)
	if err == nil {
		t.Error("expected error for non-existent offset")
	}
}

func TestSegmentSync(t *testing.T) {
	tempDir := t.TempDir()
	segment, err := NewSegment(tempDir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer segment.Close()

	// Append a message
	_, err = segment.Append([]byte("test message"), time.Now())
	if err != nil {
		t.Fatalf("failed to append message: %v", err)
	}

	// Test sync
	err = segment.Sync()
	if err != nil {
		t.Fatalf("failed to sync segment: %v", err)
	}

	if segment.LastSynced != segment.CurrentSize {
		t.Errorf("expected LastSynced %d, got %d", segment.CurrentSize, segment.LastSynced)
	}
}

func TestSegmentClose(t *testing.T) {
	tempDir := t.TempDir()
	segment, err := NewSegment(tempDir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	// Append a message
	_, err = segment.Append([]byte("test message"), time.Now())
	if err != nil {
		t.Fatalf("failed to append message: %v", err)
	}

	// Test close
	err = segment.Close()
	if err != nil {
		t.Fatalf("failed to close segment: %v", err)
	}

	// Verify files were created
	logPath := filepath.Join(tempDir, "00000000000000000000.log")
	indexPath := filepath.Join(tempDir, "00000000000000000000.index")
	timeIndexPath := filepath.Join(tempDir, "00000000000000000000.timeindex")

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("log file was not created")
	}
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("index file was not created")
	}
	if _, err := os.Stat(timeIndexPath); os.IsNotExist(err) {
		t.Error("time index file was not created")
	}
}

func TestSegmentPurgeBefore(t *testing.T) {
	tempDir := t.TempDir()
	segment, err := NewSegment(tempDir, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer segment.Close()

	// Append messages with different timestamps
	now := time.Now()
	messages := []struct {
		content   string
		timestamp time.Time
	}{
		{"old message 1", now.Add(-2 * time.Hour)},
		{"old message 2", now.Add(-1 * time.Hour)},
		{"new message 1", now.Add(-30 * time.Minute)},
		{"new message 2", now.Add(-10 * time.Minute)},
	}

	for _, msg := range messages {
		_, err := segment.Append([]byte(msg.content), msg.timestamp)
		if err != nil {
			t.Fatalf("failed to append message: %v", err)
		}
	}

	initialCount := len(segment.IndexEntries)
	if initialCount != len(messages) {
		t.Errorf("expected %d index entries, got %d", len(messages), initialCount)
	}

	// Purge messages older than 45 minutes
	purgeTime := now.Add(-45 * time.Minute)
	segment.PurgeBefore(purgeTime)

	// Should keep only the last 2 messages
	expectedCount := 2
	if len(segment.IndexEntries) != expectedCount {
		t.Errorf("expected %d index entries after purge, got %d", expectedCount, len(segment.IndexEntries))
	}
}
