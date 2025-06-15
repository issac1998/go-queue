package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"go-queue/internal/metadata"
)

const (
	// MaxMessageSize defines max message size.
	MaxMessageSize = 1 << 20 // 单条消息最大1MB
	// CompressionNone decides compress or not
	CompressionNone = 0x00
	// CompressionNone decides compress or not
	// --- 常量定义 ---
	FetchRequestType     = 0x02    // 消费请求类型标识
	DefaultMaxFetchBytes = 1 << 20 // 默认最大拉取1MB数据
	MaxFetchBytesLimit   = 5 << 20 // 服务端限制单次拉取5MB

	CurrentProtocolVersion = 1 // 协议版本
)

const (
	// 成功
	ErrorNone = 0

	// 客户端请求错误 (1-99)
	ErrorInvalidRequest   = 1 // 请求格式不合法
	ErrorInvalidTopic     = 3 // Topic 名称无效
	ErrorUnknownPartition = 5 // 分区不存在
	ErrorInvalidMessage   = 6 // 消息内容非法（如空消息）
	ErrorMessageTooLarge  = 7 // 消息超过大小限制
	ErrorOffsetOutOfRange = 8 // 请求的 Offset 超出范围

	// 服务端错误 (100-199)
	ErrorBrokerNotAvailable = 100 // Broker 不可用
	ErrorFetchFailed        = 101 // 拉取消息失败（如磁盘故障）
	ErrorProduceFailed      = 102 // 写入消息失败

	// 权限/配额错误 (200-299)
	ErrorUnauthorized  = 200 // 客户端未授权
	ErrorQuotaExceeded = 201 // 超出配额限制
)

// --- 请求/响应结构体 ---

// ProduceRequest 生产请求结构
type ProduceRequest struct {
	Topic       string
	Partition   int32
	Compression int8 // 压缩类型
	Messages    [][]byte
}

// ProduceResponse 生产响应结构
type ProduceResponse struct {
	BaseOffset int64 // 首条消息的Offset
	ErrorCode  int16 // 错误码 (0=成功)
}

// --- 编码解码方法 ---

// ReadProduceRequest 从网络连接解码请求
func ReadProduceRequest(r io.Reader) (*ProduceRequest, error) {
	var req ProduceRequest

	// 读取协议版本
	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("read version failed: %v", err)
	}

	// 读取Topic长度和内容
	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("read topic length failed: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicBytes); err != nil {
		return nil, fmt.Errorf("read topic failed: %v", err)
	}
	req.Topic = string(topicBytes)

	// 读取Partition和Compression
	if err := binary.Read(r, binary.BigEndian, &req.Partition); err != nil {
		return nil, fmt.Errorf("read partition failed: %v", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.Compression); err != nil {
		return nil, fmt.Errorf("read compression failed: %v", err)
	}

	// 读取消息集合
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
	buf := new(bytes.Buffer)

	// 写入BaseOffset和ErrorCode
	if err := binary.Write(buf, binary.BigEndian, res.BaseOffset); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, res.ErrorCode); err != nil {
		return err
	}

	// 写入总长度前缀
	totalLen := int32(buf.Len())
	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return err
	}

	_, err := w.Write(buf.Bytes())
	return err
}

// --- 请求处理逻辑 ---

// HandleProduceRequest 处理生产请求的入口函数
func HandleProduceRequest(conn io.ReadWriter) error {
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

	// 3. 获取目标分区
	partition := metadata.GetPartition(req.Topic, int32(req.Partition))
	if partition == nil {
		return writeErrorResponse(conn, 5) // 5=UNKNOWN_PARTITION
	}

	// 4. 追加消息到存储（加锁保证线程安全）
	var firstOffset int64 = -1
	partition.Mu.Lock()
	defer partition.Mu.Unlock()

	for _, msg := range req.Messages {
		offset, err := partition.Segment.Append(msg)
		if err != nil {
			return writeErrorResponse(conn, 7) // 7=MESSAGE_TOO_LARGE
		}
		if firstOffset == -1 {
			firstOffset = offset
		}
	}

	// 5. 返回成功响应
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
