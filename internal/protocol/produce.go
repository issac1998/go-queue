package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ProduceRequest represents a request to produce messages to a topic partition
type ProduceRequest struct {
	// Topic is the name of the topic to produce messages to
	Topic string
	// Partition is the partition ID to produce messages to
	Partition int32
	// Compression indicates the compression type used for messages
	Compression int8
	// Messages contains the raw message data to be produced
	Messages [][]byte
}

// ProduceResponse represents the response from a produce operation
type ProduceResponse struct {
	// BaseOffset is the offset of the first message that was written
	BaseOffset int64
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
}

// ReadProduceRequest reads and parses a produce request from the given reader
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

// Write serializes and writes the produce response to the given writer
func (res *ProduceResponse) Write(w io.Writer) error {

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

// HandleProduceRequest processes a produce request and writes the response
