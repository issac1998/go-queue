package protocol

import (
	"encoding/binary"
	"io"
	"time"
)

// CreateTopicRequest represents a request to create a topic
type CreateTopicRequest struct {
	TopicName         string
	Partitions        int32
	ReplicationFactor int32
	Config            map[string]string
}

// CreateTopicResponse represents the response from a create topic operation
type CreateTopicResponse struct {
	ErrorCode int16
	ErrorMsg  string
}

// Write serializes the CreateTopicResponse to the writer
func (r *CreateTopicResponse) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ErrorCode); err != nil {
		return err
	}

	msgBytes := []byte(r.ErrorMsg)
	if err := binary.Write(w, binary.BigEndian, int32(len(msgBytes))); err != nil {
		return err
	}
	_, err := w.Write(msgBytes)
	return err
}

// ReadCreateTopicRequest reads a CreateTopicRequest from the reader
func ReadCreateTopicRequest(r io.Reader) (*CreateTopicRequest, error) {
	var nameLen int32
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return nil, err
	}

	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return nil, err
	}

	var partitions int32
	if err := binary.Read(r, binary.BigEndian, &partitions); err != nil {
		return nil, err
	}

	var replicationFactor int32
	if err := binary.Read(r, binary.BigEndian, &replicationFactor); err != nil {
		return nil, err
	}

	return &CreateTopicRequest{
		TopicName:         string(nameBytes),
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		Config:            make(map[string]string),
	}, nil
}

// DeleteTopicRequest represents a request to delete a topic
type DeleteTopicRequest struct {
	TopicName string
}

// DeleteTopicResponse represents the response from a delete topic operation
type DeleteTopicResponse struct {
	ErrorCode int16
	ErrorMsg  string
}

// Write serializes the DeleteTopicResponse to the writer
func (r *DeleteTopicResponse) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ErrorCode); err != nil {
		return err
	}

	msgBytes := []byte(r.ErrorMsg)
	if err := binary.Write(w, binary.BigEndian, int32(len(msgBytes))); err != nil {
		return err
	}
	_, err := w.Write(msgBytes)
	return err
}

// ReadDeleteTopicRequest reads a DeleteTopicRequest from the reader
func ReadDeleteTopicRequest(r io.Reader) (*DeleteTopicRequest, error) {
	var nameLen int32
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return nil, err
	}

	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return nil, err
	}

	return &DeleteTopicRequest{
		TopicName: string(nameBytes),
	}, nil
}

// ListTopicsRequest represents a request to list all topics
type ListTopicsRequest struct {
	// Version is the protocol version
	Version int16
}

// ListTopicsResponse represents the response from a list topics operation
type ListTopicsResponse struct {
	ErrorCode int16
	Topics    []TopicInfo
}

// TopicInfo contains basic information about a topic
type TopicInfo struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
}

// Write serializes the ListTopicsResponse to the writer
func (r *ListTopicsResponse) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ErrorCode); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, int32(len(r.Topics))); err != nil {
		return err
	}

	for _, topic := range r.Topics {
		nameBytes := []byte(topic.Name)
		if err := binary.Write(w, binary.BigEndian, int32(len(nameBytes))); err != nil {
			return err
		}
		if _, err := w.Write(nameBytes); err != nil {
			return err
		}

		if err := binary.Write(w, binary.BigEndian, topic.Partitions); err != nil {
			return err
		}

		if err := binary.Write(w, binary.BigEndian, topic.ReplicationFactor); err != nil {
			return err
		}
	}

	return nil
}

// ReadListTopicsRequest reads a ListTopicsRequest from the reader
func ReadListTopicsRequest(r io.Reader) (*ListTopicsRequest, error) {
	var version int16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	return &ListTopicsRequest{
		Version: version,
	}, nil
}

// GetTopicInfoRequest represents a request to get detailed information about a topic
type GetTopicInfoRequest struct {
	TopicName string
}

// GetTopicInfoResponse represents the response from a get topic info operation
type GetTopicInfoResponse struct {
	ErrorCode int16
	Topic     TopicDetailInfo
}

// TopicDetailInfo contains detailed information about a topic
type TopicDetailInfo struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
	CreatedAt         time.Time
	Config            map[string]string
}

// Write serializes the GetTopicInfoResponse to the writer
func (r *GetTopicInfoResponse) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, r.ErrorCode); err != nil {
		return err
	}

	nameBytes := []byte(r.Topic.Name)
	if err := binary.Write(w, binary.BigEndian, int32(len(nameBytes))); err != nil {
		return err
	}
	if _, err := w.Write(nameBytes); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, r.Topic.Partitions); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, r.Topic.ReplicationFactor); err != nil {
		return err
	}

	createdAtUnix := r.Topic.CreatedAt.Unix()
	if err := binary.Write(w, binary.BigEndian, createdAtUnix); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, int32(len(r.Topic.Config))); err != nil {
		return err
	}

	for key, value := range r.Topic.Config {
		keyBytes := []byte(key)
		if err := binary.Write(w, binary.BigEndian, int32(len(keyBytes))); err != nil {
			return err
		}
		if _, err := w.Write(keyBytes); err != nil {
			return err
		}

		valueBytes := []byte(value)
		if err := binary.Write(w, binary.BigEndian, int32(len(valueBytes))); err != nil {
			return err
		}
		if _, err := w.Write(valueBytes); err != nil {
			return err
		}
	}

	return nil
}

// ReadGetTopicInfoRequest reads a GetTopicInfoRequest from the reader
func ReadGetTopicInfoRequest(r io.Reader) (*GetTopicInfoRequest, error) {
	var nameLen int32
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return nil, err
	}

	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return nil, err
	}

	return &GetTopicInfoRequest{
		TopicName: string(nameBytes),
	}, nil
}

// Legacy handlers removed - these are replaced by the new Raft-based broker handlers
// The protocol data structures above are still used by the new broker implementation
