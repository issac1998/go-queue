package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/issac1998/go-queue/internal/metadata"
	"github.com/issac1998/go-queue/internal/storage"
)

// FetchRequest represents a request to fetch messages from a topic partition
type FetchRequest struct {
	// Topic is the name of the topic to fetch messages from
	Topic string
	// Partition is the partition ID to fetch messages from
	Partition int32
	// Offset is the starting offset to fetch messages from
	Offset int64
	// MaxBytes is the maximum number of bytes to fetch
	MaxBytes int32
}

// FetchResponse represents the response from a fetch operation
type FetchResponse struct {
	// Topic is the name of the topic that was fetched from
	Topic string
	// Partition is the partition ID that was fetched from
	Partition int32
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
	// Messages contains the fetched message data
	Messages [][]byte
	// NextOffset is the offset to use for the next fetch request
	NextOffset int64
}

// ReadFetchRequest reads and parses a fetch request from the given reader
func ReadFetchRequest(r io.Reader) (*FetchRequest, error) {
	req := &FetchRequest{}

	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read protocol version: %v", err)
	}
	if version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version: %d", version)
	}

	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic: %v", err)
	}
	req.Topic = string(topicBytes)

	if err := binary.Read(r, binary.BigEndian, &req.Partition); err != nil {
		return nil, fmt.Errorf("failed to read partition: %v", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.Offset); err != nil {
		return nil, fmt.Errorf("failed to read offset: %v", err)
	}
	if err := binary.Read(r, binary.BigEndian, &req.MaxBytes); err != nil {
		return nil, fmt.Errorf("failed to read max bytes: %v", err)
	}

	if req.MaxBytes > MaxFetchBytesLimit {
		req.MaxBytes = MaxFetchBytesLimit
	} else if req.MaxBytes <= 0 {
		req.MaxBytes = DefaultMaxFetchBytes
	}

	return req, nil
}

// Write serializes and writes the fetch response to the given writer
func (res *FetchResponse) Write(w io.Writer) error {
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.BigEndian, int16(len(res.Topic)))
	headerBuf.WriteString(res.Topic)
	binary.Write(headerBuf, binary.BigEndian, res.Partition)
	binary.Write(headerBuf, binary.BigEndian, res.ErrorCode)
	binary.Write(headerBuf, binary.BigEndian, res.NextOffset)
	binary.Write(headerBuf, binary.BigEndian, int32(len(res.Messages))) // 添加消息数量

	var messagesBuf bytes.Buffer
	for _, msg := range res.Messages {
		binary.Write(&messagesBuf, binary.BigEndian, int32(len(msg)))
		messagesBuf.Write(msg)
	}

	totalLen := int32(headerBuf.Len() + messagesBuf.Len())

	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return err
	}

	if _, err := w.Write(headerBuf.Bytes()); err != nil {
		return err
	}
	if _, err := w.Write(messagesBuf.Bytes()); err != nil {
		return err
	}

	return nil
}

// HandleFetchRequest processes a fetch request and writes the response
func HandleFetchRequest(conn io.ReadWriter, manager *metadata.Manager) error {

	req, err := ReadFetchRequest(conn)
	if err != nil {
		return sendFetchError(conn, ErrorInvalidRequest, err.Error())
	}

	messages, nextOffset, err := manager.ReadMessage(req.Topic, req.Partition, req.Offset, req.MaxBytes)
	if err != nil {
		return sendFetchError(conn, ErrorOffsetOutOfRange, err.Error())
	}

	response := &FetchResponse{
		Topic:      req.Topic,
		Partition:  req.Partition,
		ErrorCode:  ErrorNone,
		Messages:   messages,
		NextOffset: nextOffset,
	}
	return response.Write(conn)
}

// findSegmentAndPosition
func findSegmentAndPosition(p *metadata.Partition, offset int64) (*storage.Segment, int64, error) {
	for _, seg := range p.Segments {
		if offset >= seg.BaseOffset && offset < seg.BaseOffset+seg.MaxBytes {
			pos, err := seg.FindPosition(offset)
			return seg, pos, err
		}
	}
	return nil, 0, errors.New("offset out of range")
}

// readMessagesFromSegment
func readMessagesFromSegment(
	seg *storage.Segment,
	startPos int64,
	maxBytes int64,
) ([][]byte, int64, error) {
	var messages [][]byte
	currentPos := startPos
	totalBytes := int64(0)

	for totalBytes < maxBytes {
		var msgSize int32
		lenBuf := make([]byte, 4)
		if _, err := seg.ReadAt(currentPos, lenBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, 0, err
		}
		msgSize = int32(binary.BigEndian.Uint32(lenBuf))

		currentPos += 4

		if msgSize <= 0 || int64(msgSize) > (maxBytes-totalBytes) {
			break
		}

		msg := make([]byte, msgSize)
		if _, err := seg.ReadAt(currentPos, msg); err != nil {
			return nil, 0, err
		}
		messages = append(messages, msg)
		currentPos += int64(msgSize)
		totalBytes += int64(msgSize) + 4
	}

	return messages, seg.BaseOffset + currentPos, nil
}

// sendFetchError
func sendFetchError(w io.Writer, code int16, message string) error {
	response := &FetchResponse{
		ErrorCode: code,
	}
	if err := response.Write(w); err != nil {
		return fmt.Errorf("failed to send error response: %v", err)
	}
	return fmt.Errorf("fetch error %d: %s", code, message)
}
