package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/issac1998/go-queue/internal/metadata"
)

const (
	// MaxMessageSize defines max message size.
	MaxMessageSize = 1 << 20
	// CompressionNone decides compress or not
	CompressionNone = 0x00
	// CompressionNone decides compress or not
	FetchRequestType = 0x01

	DefaultMaxFetchBytes = 1 << 20
	MaxFetchBytesLimit   = 5 << 20

	CurrentProtocolVersion = 1
)

const (
	ErrorNone = 0

	ErrorInvalidRequest   = 1
	ErrorInvalidTopic     = 2
	ErrorUnknownPartition = 3
	ErrorInvalidMessage   = 4
	ErrorMessageTooLarge  = 5
	ErrorOffsetOutOfRange = 6

	ErrorBrokerNotAvailable = 100
	ErrorFetchFailed        = 101
	ErrorProduceFailed      = 102

	ErrorUnauthorized  = 200
	ErrorQuotaExceeded = 201
)

// ProduceRequest
type ProduceRequest struct {
	Topic       string
	Partition   int32
	Compression int8
	Messages    [][]byte
}

// ProduceResponse
type ProduceResponse struct {
	BaseOffset int64
	ErrorCode  int16
}

// ReadProduceRequest decodes request from network connection
func ReadProduceRequest(r io.Reader) (*ProduceRequest, error) {
	var req ProduceRequest

	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("read version failed: %v", err)
	}

	// Read topic length and content
	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("read topic length failed: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicBytes); err != nil {
		return nil, fmt.Errorf("read topic failed: %v", err)
	}
	req.Topic = string(topicBytes)

	// Read partition and compression
	if err := binary.Read(r, binary.BigEndian, &req.Partition); err != nil {
		return nil, fmt.Errorf("read partition failed: %v", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.Compression); err != nil {
		return nil, fmt.Errorf("read compression failed: %v", err)
	}

	// Read message set
	var msgSetSize int32
	if err := binary.Read(r, binary.BigEndian, &msgSetSize); err != nil {
		return nil, fmt.Errorf("read message set size failed: %v", err)
	}

	limitedReader := io.LimitReader(r, int64(msgSetSize))
	for {
		var msgSize int32
		if err := binary.Read(limitedReader, binary.BigEndian, &msgSize); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("read message size failed: %v", err)
		}

		if msgSize > MaxMessageSize {
			return nil, fmt.Errorf("message size %d exceeds limit %d", msgSize, MaxMessageSize)
		}

		msg := make([]byte, msgSize)
		if _, err := io.ReadFull(limitedReader, msg); err != nil {
			return nil, fmt.Errorf("read message content failed: %v", err)
		}
		req.Messages = append(req.Messages, msg)
	}

	return &req, nil
}

// WriteProduceResponse
func (res *ProduceResponse) Write(w io.Writer) error {
	// 先写入总长度前缀 (8 + 2 = 10 bytes)
	responseLen := int32(8 + 2) // BaseOffset(8) + ErrorCode(2)
	if err := binary.Write(w, binary.BigEndian, responseLen); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, res.BaseOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, res.ErrorCode); err != nil {
		return err
	}

	return nil
}

// HandleProduceRequest
func HandleProduceRequest(conn io.ReadWriter, manager *metadata.Manager, clusterManager interface{}) error {
	req, err := ReadProduceRequest(conn)
	if err != nil {
		return fmt.Errorf("invalid produce request: %v", err)
	}

	if req.Topic == "" {
		return writeErrorResponse(conn, ErrorInvalidTopic)
	}
	if len(req.Messages) == 0 {
		return writeErrorResponse(conn, ErrorInvalidMessage)
	}

	// 如果启用了集群模式，优先使用集群管理器
	if clusterManager != nil {
		// 这里可以添加集群相关的逻辑
		// 目前先使用本地manager
	}

	var firstOffset int64 = -1
	for _, msg := range req.Messages {
		offset, err := manager.WriteMessage(req.Topic, req.Partition, msg)
		if err != nil {
			return writeErrorResponse(conn, ErrorMessageTooLarge)
		}
		if firstOffset == -1 {
			firstOffset = offset
		}
	}

	response := &ProduceResponse{
		BaseOffset: firstOffset,
		ErrorCode:  0,
	}
	return response.Write(conn)
}

// writeErrorResponse
func writeErrorResponse(w io.Writer, errorCode int16) error {
	response := &ProduceResponse{
		ErrorCode: errorCode,
	}
	return response.Write(w)
}

func HandleCreateTopicRequest(conn net.Conn, manager *metadata.Manager) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		writeErrorResponse(conn, ErrorInvalidRequest)
		return
	}

	var nameLen int16
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		writeErrorResponse(conn, ErrorInvalidRequest)
		return
	}

	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		writeErrorResponse(conn, ErrorInvalidRequest)
		return
	}
	topicName := string(nameBuf)

	var partitions int32
	if err := binary.Read(conn, binary.BigEndian, &partitions); err != nil {
		writeErrorResponse(conn, ErrorInvalidRequest)
		return
	}

	var replicas int32 = 1
	binary.Read(conn, binary.BigEndian, &replicas)

	_, err := manager.CreateTopic(topicName, &metadata.TopicConfig{
		Partitions: partitions,
		Replicas:   replicas,
	})
	if err != nil {
		writeErrorResponse(conn, ErrorInvalidRequest)
		return
	}

	writeSuccessResponse(conn)
}

func writeSuccessResponse(conn net.Conn) {
	writeErrorResponse(conn, 0)
}
