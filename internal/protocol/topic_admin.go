package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/issac1998/go-queue/internal/metadata"
)

// ListTopicsRequest represents a request to list all available topics
type ListTopicsRequest struct {
}

// ListTopicsResponse represents the response containing all available topics
type ListTopicsResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
	// Topics contains the list of all topics with their information
	Topics []metadata.TopicInfo
}

// TopicInfo contains detailed information about a topic (protocol-level definition)
type TopicInfo struct {
	// Name is the topic name
	Name string
	// Partitions is the number of partitions in the topic
	Partitions int32
	// Replicas is the number of replicas per partition
	Replicas int32
	// CreatedAt is when the topic was created
	CreatedAt time.Time
	// Size is the total size of the topic in bytes
	Size int64
	// MessageCount is the total number of messages in the topic
	MessageCount int64
	// PartitionDetails contains detailed information about each partition
	PartitionDetails []PartitionInfo
}

// PartitionInfo contains detailed information about a partition (protocol-level definition)
type PartitionInfo struct {
	// ID is the partition identifier
	ID int32
	// Leader is the leader replica ID
	Leader int32
	// Replicas is the list of replica IDs
	Replicas []int32
	// ISR is the list of in-sync replica IDs
	ISR []int32
	// Size is the partition size in bytes
	Size int64
	// MessageCount is the number of messages in the partition
	MessageCount int64
	// StartOffset is the earliest available offset
	StartOffset int64
	// EndOffset is the next offset to be assigned
	EndOffset int64
}

// GetTopicRequest represents a request to get detailed information about a topic
type GetTopicRequest struct {
	TopicName string
}

// GetTopicResponse represents the response containing detailed topic information
type GetTopicResponse struct {
	ErrorCode int16
	Topic     *metadata.TopicInfo
}

// DeleteTopicRequest represents a request to delete a topic
type DeleteTopicRequest struct {
	// TopicName is the name of the topic to delete
	TopicName string
}

// DeleteTopicResponse represents the response after deleting a topic
type DeleteTopicResponse struct {
	// ErrorCode indicates whether the operation succeeded (0) or failed (non-zero)
	ErrorCode int16
	// TopicName is the name of the deleted topic
	TopicName string
}

func ReadListTopicsRequest(r io.Reader) (*ListTopicsRequest, error) {
	req := &ListTopicsRequest{}

	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %v", err)
	}

	return req, nil
}

func (res *ListTopicsResponse) Write(w io.Writer) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, res.ErrorCode); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(len(res.Topics))); err != nil {
		return err
	}

	for _, topic := range res.Topics {

		if err := binary.Write(buf, binary.BigEndian, int16(len(topic.Name))); err != nil {
			return err
		}
		if _, err := buf.WriteString(topic.Name); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.BigEndian, topic.Partitions); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.Replicas); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.CreatedAt.Unix()); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.Size); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.MessageCount); err != nil {
			return err
		}
	}

	totalLen := int32(buf.Len())
	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return err
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func ReadDescribeTopicRequest(r io.Reader) (*GetTopicRequest, error) {
	req := &GetTopicRequest{}

	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %v", err)
	}

	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic name: %v", err)
	}
	req.TopicName = string(topicBytes)

	return req, nil
}

func (res *GetTopicResponse) Write(w io.Writer) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, res.ErrorCode); err != nil {
		return err
	}

	if res.ErrorCode == ErrorNone && res.Topic != nil {
		topic := res.Topic

		if err := binary.Write(buf, binary.BigEndian, int16(len(topic.Name))); err != nil {
			return err
		}
		if _, err := buf.WriteString(topic.Name); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.BigEndian, topic.Partitions); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.Replicas); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.CreatedAt.Unix()); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.Size); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, topic.MessageCount); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.BigEndian, int32(len(topic.PartitionDetails))); err != nil {
			return err
		}

		for _, partition := range topic.PartitionDetails {
			if err := binary.Write(buf, binary.BigEndian, partition.ID); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, partition.Leader); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, partition.Size); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, partition.MessageCount); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, partition.StartOffset); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, partition.EndOffset); err != nil {
				return err
			}
		}
	}

	totalLen := int32(buf.Len())
	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return err
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func ReadDeleteTopicRequest(r io.Reader) (*DeleteTopicRequest, error) {
	req := &DeleteTopicRequest{}

	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %v", err)
	}

	var topicLen int16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, fmt.Errorf("failed to read topic length: %v", err)
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicBytes); err != nil {
		return nil, fmt.Errorf("failed to read topic name: %v", err)
	}
	req.TopicName = string(topicBytes)

	return req, nil
}

func (res *DeleteTopicResponse) Write(w io.Writer) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, res.ErrorCode); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.BigEndian, int16(len(res.TopicName))); err != nil {
		return err
	}
	if _, err := buf.WriteString(res.TopicName); err != nil {
		return err
	}

	totalLen := int32(buf.Len())
	if err := binary.Write(w, binary.BigEndian, totalLen); err != nil {
		return err
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// HandleListTopicsRequest processes a list topics request and writes the response
func HandleListTopicsRequest(conn net.Conn, manager *metadata.Manager) error {
	defer conn.Close()

	_, err := ReadListTopicsRequest(conn)
	if err != nil {
		return sendTopicError(conn, ErrorInvalidRequest, "Failed to read request")
	}

	topics, err := manager.ListTopicsDetailed()
	if err != nil {
		return sendTopicError(conn, ErrorBrokerNotAvailable, "Failed to list topics")
	}

	response := &ListTopicsResponse{
		ErrorCode: ErrorNone,
		Topics:    topics,
	}

	return response.Write(conn)
}

// HandleDescribeTopicRequest processes a describe topic request and writes the response
func HandleGetTopicInfoRequest(conn net.Conn, manager *metadata.Manager) error {
	defer conn.Close()

	req, err := ReadDescribeTopicRequest(conn)
	if err != nil {
		return sendTopicError(conn, ErrorInvalidRequest, "Failed to read request")
	}

	topic, err := manager.GetTopicInfo(req.TopicName)
	if err != nil {
		return sendTopicError(conn, ErrorInvalidTopic, "Topic not found")
	}

	response := &GetTopicResponse{
		ErrorCode: ErrorNone,
		Topic:     topic,
	}

	return response.Write(conn)
}

// HandleDeleteTopicRequest processes a delete topic request and writes the response
func HandleDeleteTopicRequest(conn net.Conn, manager *metadata.Manager) error {
	defer conn.Close()

	req, err := ReadDeleteTopicRequest(conn)
	if err != nil {
		return sendTopicError(conn, ErrorInvalidRequest, "Failed to read request")
	}

	err = manager.DeleteTopic(req.TopicName)
	if err != nil {
		return sendTopicError(conn, ErrorInvalidTopic, "Failed to delete topic")
	}

	response := &DeleteTopicResponse{
		ErrorCode: ErrorNone,
		TopicName: req.TopicName,
	}

	return response.Write(conn)
}

func sendTopicError(conn net.Conn, errorCode int16, message string) error {
	response := &ListTopicsResponse{
		ErrorCode: errorCode,
		Topics:    nil,
	}
	return response.Write(conn)
}
