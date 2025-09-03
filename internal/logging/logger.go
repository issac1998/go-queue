package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// LogLevel represents the logging level
type LogLevel string

const (
	LevelDebug LogLevel = "debug"
	LevelInfo  LogLevel = "info"
	LevelWarn  LogLevel = "warn"
	LevelError LogLevel = "error"
)

// LogFormat represents the logging format
type LogFormat string

const (
	FormatJSON LogFormat = "json"
	FormatText LogFormat = "text"
)

// Config represents the logging configuration
type Config struct {
	Level         LogLevel  `json:"level" yaml:"level"`
	Format        LogFormat `json:"format" yaml:"format"`
	OutputFile    string    `json:"output_file" yaml:"output_file"`
	EnableConsole bool      `json:"enable_console" yaml:"enable_console"`
}

// Logger wraps slog.Logger with additional context
type Logger struct {
	*slog.Logger
	config Config
	file   *os.File
}

// New creates a new structured logger
func New(config Config) (*Logger, error) {
	var writers []io.Writer
	var file *os.File

	// Add console output if enabled
	if config.EnableConsole {
		writers = append(writers, os.Stdout)
	}

	// Add file output if specified
	if config.OutputFile != "" {
		// Ensure directory exists
		dir := filepath.Dir(config.OutputFile)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		var err error
		file, err = os.OpenFile(config.OutputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		writers = append(writers, file)
	}

	// Default to stdout if no outputs specified
	if len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}

	// Create multi-writer
	var output io.Writer
	if len(writers) == 1 {
		output = writers[0]
	} else {
		output = io.MultiWriter(writers...)
	}

	// Convert level
	var level slog.Level
	switch config.Level {
	case LevelDebug:
		level = slog.LevelDebug
	case LevelInfo:
		level = slog.LevelInfo
	case LevelWarn:
		level = slog.LevelWarn
	case LevelError:
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	// Create handler based on format
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Format time as RFC3339
			if a.Key == slog.TimeKey {
				return slog.String(slog.TimeKey, a.Value.Time().Format(time.RFC3339))
			}
			return a
		},
	}

	switch config.Format {
	case FormatJSON:
		handler = slog.NewJSONHandler(output, opts)
	case FormatText:
		handler = slog.NewTextHandler(output, opts)
	default:
		handler = slog.NewTextHandler(output, opts)
	}

	logger := &Logger{
		Logger: slog.New(handler),
		config: config,
		file:   file,
	}

	return logger, nil
}

// Close closes any open file handles
func (l *Logger) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// WithComponent creates a logger with a component context
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{
		Logger: l.Logger.With("component", component),
		config: l.config,
		file:   l.file,
	}
}

// WithBrokerID creates a logger with broker ID context
func (l *Logger) WithBrokerID(brokerID string) *Logger {
	return &Logger{
		Logger: l.Logger.With("broker_id", brokerID),
		config: l.config,
		file:   l.file,
	}
}

// WithRaftGroup creates a logger with Raft group context
func (l *Logger) WithRaftGroup(groupID uint64) *Logger {
	return &Logger{
		Logger: l.Logger.With("raft_group", groupID),
		config: l.config,
		file:   l.file,
	}
}

// WithPartition creates a logger with partition context
func (l *Logger) WithPartition(topic string, partition int32) *Logger {
	return &Logger{
		Logger: l.Logger.With("topic", topic, "partition", partition),
		config: l.config,
		file:   l.file,
	}
}

// WithError creates a logger with error context
func (l *Logger) WithError(err error) *Logger {
	return &Logger{
		Logger: l.Logger.With("error", err.Error()),
		config: l.config,
		file:   l.file,
	}
}

// Convenience methods for common log patterns

// InfoContext logs an info message with structured context
func (l *Logger) InfoContext(msg string, args ...any) {
	l.Info(msg, args...)
}

// ErrorContext logs an error message with structured context
func (l *Logger) ErrorContext(msg string, err error, args ...any) {
	allArgs := append([]any{"error", err}, args...)
	l.Error(msg, allArgs...)
}

// WarnContext logs a warning message with structured context
func (l *Logger) WarnContext(msg string, args ...any) {
	l.Warn(msg, args...)
}

// DebugContext logs a debug message with structured context
func (l *Logger) DebugContext(msg string, args ...any) {
	l.Debug(msg, args...)
}

// StartupInfo logs startup information
func (l *Logger) StartupInfo(component string, details map[string]any) {
	args := []any{"event", "startup", "component", component}
	for k, v := range details {
		args = append(args, k, v)
	}
	l.Info("Component starting", args...)
}

// ShutdownInfo logs shutdown information
func (l *Logger) ShutdownInfo(component string, details map[string]any) {
	args := []any{"event", "shutdown", "component", component}
	for k, v := range details {
		args = append(args, k, v)
	}
	l.Info("Component stopping", args...)
}

// Performance logs performance metrics
func (l *Logger) Performance(operation string, duration time.Duration, details map[string]any) {
	args := []any{
		"event", "performance",
		"operation", operation,
		"duration_ms", duration.Milliseconds(),
	}
	for k, v := range details {
		args = append(args, k, v)
	}
	l.Info("Performance metrics", args...)
}

// LeadershipChange logs leadership change events
func (l *Logger) LeadershipChange(groupID uint64, oldLeader, newLeader string, details map[string]any) {
	args := []any{
		"event", "leadership_change",
		"raft_group", groupID,
		"old_leader", oldLeader,
		"new_leader", newLeader,
	}
	for k, v := range details {
		args = append(args, k, v)
	}
	l.Info("Leadership changed", args...)
}

// PartitionOperation logs partition-related operations
func (l *Logger) PartitionOperation(operation, topic string, partition int32, details map[string]any) {
	args := []any{
		"event", "partition_operation",
		"operation", operation,
		"topic", topic,
		"partition", partition,
	}
	for k, v := range details {
		args = append(args, k, v)
	}
	l.Info("Partition operation", args...)
}

// ClientRequest logs client request information
func (l *Logger) ClientRequest(requestType string, clientAddr string, details map[string]any) {
	args := []any{
		"event", "client_request",
		"request_type", requestType,
		"client_addr", clientAddr,
	}
	for k, v := range details {
		args = append(args, k, v)
	}
	l.Debug("Client request", args...)
}

// RaftOperation logs Raft-related operations
func (l *Logger) RaftOperation(operation string, groupID uint64, details map[string]any) {
	args := []any{
		"event", "raft_operation",
		"operation", operation,
		"raft_group", groupID,
	}
	for k, v := range details {
		args = append(args, k, v)
	}
	l.Debug("Raft operation", args...)
}

// Global logger instance
var defaultLogger *Logger

// Initialize sets up the global logger
func Initialize(config Config) error {
	logger, err := New(config)
	if err != nil {
		return err
	}
	defaultLogger = logger
	return nil
}

// GetLogger returns the global logger instance
func GetLogger() *Logger {
	if defaultLogger == nil {
		// Fallback to a basic logger if not initialized
		config := Config{
			Level:         LevelInfo,
			Format:        FormatText,
			EnableConsole: true,
		}
		logger, _ := New(config)
		defaultLogger = logger
	}
	return defaultLogger
}

// Close closes the global logger
func Close() error {
	if defaultLogger != nil {
		return defaultLogger.Close()
	}
	return nil
}
