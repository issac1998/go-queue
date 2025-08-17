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
	MaxMessageSize = 1 << 20 // 单条消息最大1MB
	// CompressionNone decides compress or not
	CompressionNone = 0x00
	// CompressionNone decides compress or not
	// --- 常量定义 ---
	FetchRequestType     = 0x01    // 消费请求类型标识
	DefaultMaxFetchBytes = 1 << 20 // 默认最大拉取1MB数据
	MaxFetchBytesLimit   = 5 << 20 // 服务端限制单次拉取5MB

	CurrentProtocolVersion = 1 // 协议版本
)

const (
	// Success
	ErrorNone = 0

	// Client request errors (1-99)
	ErrorInvalidRequest   = 1 // Invalid request format
	ErrorInvalidTopic     = 3 // Invalid topic name
	ErrorUnknownPartition = 5 // Partition does not exist
	ErrorInvalidMessage   = 6 // Invalid message content (e.g., empty message)
	ErrorMessageTooLarge  = 7 // Message exceeds size limit
	ErrorOffsetOutOfRange = 8 // Requested offset is out of range

	// Server errors (100-199)
	ErrorBrokerNotAvailable = 100 // Broker is unavailable
	ErrorFetchFailed        = 101 // Failed to fetch messages (e.g., disk failure)
	ErrorProduceFailed      = 102 // Failed to write messages

	// Permission/quota errors (200-299)
	ErrorUnauthorized  = 200 // Client is unauthorized
	ErrorQuotaExceeded = 201 // Quota limit exceeded
)

// --- Request/Response structures ---

// ProduceRequest produce request structure
type ProduceRequest struct {
	Topic       string
	Partition   int32
	Compression int8 // Compression type
	Messages    [][]byte
}

// ProduceResponse produce response structure
type ProduceResponse struct {
	BaseOffset int64 // Offset of the first message
	ErrorCode  int16 // Error code (0=success)
}

// --- Encoding/Decoding methods ---

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

	// 限制读取大小防止内存耗尽
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

// WriteProduceResponse 将响应编码到网络连接
func (res *ProduceResponse) Write(w io.Writer) error {
	// 先写入总长度前缀 (8 + 2 = 10 bytes)
	responseLen := int32(8 + 2) // BaseOffset(8) + ErrorCode(2)
	if err := binary.Write(w, binary.BigEndian, responseLen); err != nil {
		return err
	}

	// 写入BaseOffset和ErrorCode
	if err := binary.Write(w, binary.BigEndian, res.BaseOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, res.ErrorCode); err != nil {
		return err
	}

	return nil
}

// --- 请求处理逻辑 ---

// HandleProduceRequest 处理生产请求的入口函数
func HandleProduceRequest(conn io.ReadWriter, manager *metadata.Manager) error {
	// 1. 读取并解码请求
	req, err := ReadProduceRequest(conn)
	if err != nil {
		return fmt.Errorf("invalid produce request: %v", err)
	}

	// 2. 验证请求参数
	if req.Topic == "" {
		return writeErrorResponse(conn, 3) // 3=INVALID_TOPIC
	}
	if len(req.Messages) == 0 {
		return writeErrorResponse(conn, 6) // 6=INVALID_MESSAGE
	}

	// 3. 批量写入消息到Manager
	var firstOffset int64 = -1
	for _, msg := range req.Messages {
		offset, err := manager.WriteMessage(req.Topic, req.Partition, msg)
		if err != nil {
			return writeErrorResponse(conn, 7) // 7=MESSAGE_TOO_LARGE或其他错误
		}
		if firstOffset == -1 {
			firstOffset = offset
		}
	}

	// 4. 返回成功响应
	response := &ProduceResponse{
		BaseOffset: firstOffset,
		ErrorCode:  0,
	}
	return response.Write(conn)
}

// writeErrorResponse 辅助函数：写入错误响应
func writeErrorResponse(w io.Writer, errorCode int16) error {
	response := &ProduceResponse{
		ErrorCode: errorCode,
	}
	return response.Write(w)
}

func HandleCreateTopicRequest(conn net.Conn, manager *metadata.Manager) {
	// 1. 读取协议版本
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		writeErrorResponse(conn, 1) // 1为自定义错误码
		return
	}

	// 2. 读取 topic 名长度
	var nameLen int16
	if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
		writeErrorResponse(conn, 1) // 1为自定义错误码
		return
	}

	// 3. 读取 topic 名
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		writeErrorResponse(conn, 1) // 1为自定义错误码
		return
	}
	topicName := string(nameBuf)

	// 4. 读取分区数
	var partitions int32
	if err := binary.Read(conn, binary.BigEndian, &partitions); err != nil {
		writeErrorResponse(conn, 1) // 1为自定义错误码
		return
	}

	// 5. 读取副本数（可选）
	var replicas int32 = 1
	binary.Read(conn, binary.BigEndian, &replicas) // 忽略错误，默认1

	// 6. 创建 topic
	_, err := manager.CreateTopic(topicName, &metadata.TopicConfig{
		Partitions: partitions,
		Replicas:   replicas,
	})
	if err != nil {
		writeErrorResponse(conn, 1) // 1为自定义错误码
		return
	}

	// 7. 返回成功响应
	writeSuccessResponse(conn)
}

func writeSuccessResponse(conn net.Conn) {
	writeErrorResponse(conn, 0) // 0 表示成功
}
