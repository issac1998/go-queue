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
	MaxMessageSize = 1 << 20
	// CompressionNone decides compress or not
	CompressionNone = 0x00
	// CompressionNone decides compress or not
	FetchRequestType     = 0x02
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

// WriteProduceResponse
func (res *ProduceResponse) Write(w io.Writer) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, res.BaseOffset); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, res.ErrorCode); err != nil {
		return err
	}

	totalLen := int32(buf.Len())
	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return err
	}

	_, err := w.Write(buf.Bytes())
	return err
}

// HandleProduceRequest
func HandleProduceRequest(conn io.ReadWriter) error {
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

	partition := metadata.GetPartition(req.Topic, int32(req.Partition))
	if partition == nil {
		return writeErrorResponse(conn, ErrorUnknownPartition)
	}

	var firstOffset int64 = -1
	partition.Mu.Lock()
	defer partition.Mu.Unlock()

	for _, msg := range req.Messages {
		offset, err := partition.Segment.Append(msg)
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
