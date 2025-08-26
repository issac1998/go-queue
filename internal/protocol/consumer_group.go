package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"

	"github.com/issac1998/go-queue/internal/metadata"
)

// Consumer Group相关的请求类型
const (
	JoinGroupRequestType     = 3
	LeaveGroupRequestType    = 4
	HeartbeatRequestType     = 5
	CommitOffsetRequestType  = 6
	FetchOffsetRequestType   = 7
	ListGroupsRequestType    = 8
	DescribeGroupRequestType = 9
)

// JoinGroupRequest 加入组请求
type JoinGroupRequest struct {
	GroupID        string
	ConsumerID     string
	ClientID       string
	Topics         []string
	SessionTimeout time.Duration
}

// JoinGroupResponse 加入组响应
type JoinGroupResponse struct {
	ErrorCode  int16
	Generation int32
	GroupID    string
	ConsumerID string
	Leader     string
	Members    []GroupMember
	Assignment map[string][]int32 // Topic -> Partitions
}

// GroupMember 组成员信息
type GroupMember struct {
	ID       string
	ClientID string
}

// LeaveGroupRequest 离开组请求
type LeaveGroupRequest struct {
	GroupID    string
	ConsumerID string
}

// LeaveGroupResponse 离开组响应
type LeaveGroupResponse struct {
	ErrorCode int16
}

// HeartbeatRequest 心跳请求
type HeartbeatRequest struct {
	GroupID    string
	ConsumerID string
	Generation int32
}

// HeartbeatResponse 心跳响应
type HeartbeatResponse struct {
	ErrorCode int16
}

// CommitOffsetRequest 提交offset请求
type CommitOffsetRequest struct {
	GroupID   string
	TopicName string
	Partition int32
	Offset    int64
	Metadata  string
}

// CommitOffsetResponse 提交offset响应
type CommitOffsetResponse struct {
	ErrorCode int16
}

// FetchOffsetRequest 获取offset请求
type FetchOffsetRequest struct {
	GroupID   string
	TopicName string
	Partition int32
}

// FetchOffsetResponse 获取offset响应
type FetchOffsetResponse struct {
	ErrorCode int16
	Offset    int64
}

// HandleJoinGroupRequest 处理加入组请求
func HandleJoinGroupRequest(conn net.Conn, manager *metadata.Manager, clusterManager interface{}) error {
	req, err := parseJoinGroupRequest(conn)
	if err != nil {
		return sendJoinGroupError(conn, 1) // 协议错误
	}

	log.Printf("Handling JoinGroup request: GroupID=%s, ConsumerID=%s, Topics=%v",
		req.GroupID, req.ConsumerID, req.Topics)

	var consumer *metadata.Consumer
	var group *metadata.ConsumerGroup
	var needRebalance bool
	var isLeader bool

	// 检查是否为集群模式
	if clusterManager != nil {
		// 集群模式：使用集群管理器
		if cm, ok := clusterManager.(interface {
			JoinConsumerGroup(groupID, consumerID, clientID string, topics []string, sessionTimeout time.Duration) (*metadata.Consumer, error)
		}); ok {
			consumer, err = cm.JoinConsumerGroup(req.GroupID, req.ConsumerID, req.ClientID, req.Topics, req.SessionTimeout)
			if err != nil {
				log.Printf("Failed to join group in cluster mode: %v", err)
				return sendJoinGroupError(conn, 2) // 服务器错误
			}

			// 在集群模式下，重平衡逻辑稍有不同
			// 需要从集群状态中获取组信息
			// 这里暂时使用简化逻辑
			needRebalance = true
			isLeader = true // 假设当前节点是leader
		} else {
			log.Printf("Cluster manager does not support consumer groups")
			return sendJoinGroupError(conn, 2)
		}
	} else {
		// 单机模式：使用本地manager
		consumer, err = manager.ConsumerGroups.JoinGroup(
			req.GroupID, req.ConsumerID, req.ClientID, req.Topics, req.SessionTimeout)
		if err != nil {
			log.Printf("Failed to join group: %v", err)
			return sendJoinGroupError(conn, 2) // 服务器错误
		}

		// 检查是否需要重平衡
		group, exists, unlock := manager.ConsumerGroups.GetGroupInfo(req.GroupID)
		if !exists {
			return sendJoinGroupError(conn, 3) // 组不存在
		}
		needRebalance = group.State == metadata.GroupStatePreparingRebalance
		isLeader = group.Leader == req.ConsumerID
		unlock()
	}

	// 如果需要重平衡且当前消费者是Leader，执行重平衡
	if needRebalance && isLeader {
		topicPartitions := make(map[string][]int32)

		// 获取订阅Topic的分区信息
		for _, topicName := range req.Topics {
			if topic, exists := manager.Topics[topicName]; exists {
				var partitions []int32
				for partitionID := range topic.Partitions {
					partitions = append(partitions, partitionID)
				}
				topicPartitions[topicName] = partitions
			}
		}

		if clusterManager != nil {
			// 集群模式重平衡
			if cm, ok := clusterManager.(interface {
				RebalanceConsumerGroupPartitions(groupID string, assignment map[string][]int32) error
			}); ok {
				// 简化的轮询分配算法
				assignment := make(map[string][]int32)
				for topicName, partitions := range topicPartitions {
					assignment[req.ConsumerID] = partitions // 简化：全部分配给当前消费者
					consumer.Assignment[topicName] = partitions
				}

				err = cm.RebalanceConsumerGroupPartitions(req.GroupID, assignment)
				if err != nil {
					log.Printf("Failed to rebalance partitions in cluster mode: %v", err)
				}
			}
		} else {
			// 单机模式重平衡
			err = manager.ConsumerGroups.RebalancePartitions(req.GroupID, topicPartitions)
			if err != nil {
				log.Printf("Failed to rebalance partitions: %v", err)
			}
		}
	}

	// 重新获取组信息（单机模式）或构建响应（集群模式）
	if clusterManager == nil {
		groupInfo, exists, unlock := manager.ConsumerGroups.GetGroupInfo(req.GroupID)
		if !exists {
			return sendJoinGroupError(conn, 3) // 组不存在
		}
		defer unlock()
		group = groupInfo
	} else {
		// 集群模式：构建临时组信息用于响应
		group = &metadata.ConsumerGroup{
			ID:         req.GroupID,
			Generation: 1, // 简化
			Leader:     req.ConsumerID,
			Members:    map[string]*metadata.Consumer{req.ConsumerID: consumer},
		}
	}

	// 构建响应
	response := &JoinGroupResponse{
		ErrorCode:  0,
		Generation: group.Generation,
		GroupID:    group.ID,
		ConsumerID: consumer.ID,
		Leader:     group.Leader,
		Assignment: consumer.Assignment,
	}

	// 添加组成员信息
	for _, member := range group.Members {
		response.Members = append(response.Members, GroupMember{
			ID:       member.ID,
			ClientID: member.ClientID,
		})
	}

	return sendJoinGroupResponse(conn, response)
}

// HandleLeaveGroupRequest 处理离开组请求
func HandleLeaveGroupRequest(conn net.Conn, manager *metadata.Manager, clusterManager interface{}) error {
	req, err := parseLeaveGroupRequest(conn)
	if err != nil {
		return sendLeaveGroupError(conn, 1)
	}

	log.Printf("Handling LeaveGroup request: GroupID=%s, ConsumerID=%s", req.GroupID, req.ConsumerID)

	if clusterManager != nil {
		// 集群模式
		if cm, ok := clusterManager.(interface {
			LeaveConsumerGroup(groupID, consumerID string) error
		}); ok {
			err = cm.LeaveConsumerGroup(req.GroupID, req.ConsumerID)
			if err != nil {
				log.Printf("Failed to leave group in cluster mode: %v", err)
				return sendLeaveGroupError(conn, 2)
			}
		} else {
			return sendLeaveGroupError(conn, 2)
		}
	} else {
		// 单机模式
		err = manager.ConsumerGroups.LeaveGroup(req.GroupID, req.ConsumerID)
		if err != nil {
			log.Printf("Failed to leave group: %v", err)
			return sendLeaveGroupError(conn, 2)
		}
	}

	return sendLeaveGroupResponse(conn, &LeaveGroupResponse{ErrorCode: 0})
}

// HandleHeartbeatRequest 处理心跳请求
func HandleHeartbeatRequest(conn net.Conn, manager *metadata.Manager, clusterManager interface{}) error {
	req, err := parseHeartbeatRequest(conn)
	if err != nil {
		return sendHeartbeatError(conn, 1)
	}

	if clusterManager != nil {
		// 集群模式
		if cm, ok := clusterManager.(interface {
			HeartbeatConsumerGroup(groupID, consumerID string) error
		}); ok {
			err = cm.HeartbeatConsumerGroup(req.GroupID, req.ConsumerID)
			if err != nil {
				log.Printf("Heartbeat failed in cluster mode: %v", err)
				return sendHeartbeatError(conn, 2)
			}
		} else {
			return sendHeartbeatError(conn, 2)
		}
	} else {
		// 单机模式
		err = manager.ConsumerGroups.Heartbeat(req.GroupID, req.ConsumerID)
		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
			return sendHeartbeatError(conn, 2)
		}
	}

	return sendHeartbeatResponse(conn, &HeartbeatResponse{ErrorCode: 0})
}

// HandleCommitOffsetRequest 处理提交offset请求
func HandleCommitOffsetRequest(conn net.Conn, manager *metadata.Manager, clusterManager interface{}) error {
	req, err := parseCommitOffsetRequest(conn)
	if err != nil {
		return sendCommitOffsetError(conn, 1)
	}

	log.Printf("Handling CommitOffset request: GroupID=%s, Topic=%s, Partition=%d, Offset=%d",
		req.GroupID, req.TopicName, req.Partition, req.Offset)

	if clusterManager != nil {
		// 集群模式
		if cm, ok := clusterManager.(interface {
			CommitConsumerGroupOffset(groupID, topic string, partition int32, offset int64) error
		}); ok {
			err = cm.CommitConsumerGroupOffset(req.GroupID, req.TopicName, req.Partition, req.Offset)
			if err != nil {
				log.Printf("Failed to commit offset in cluster mode: %v", err)
				return sendCommitOffsetError(conn, 2)
			}
		} else {
			return sendCommitOffsetError(conn, 2)
		}
	} else {
		// 单机模式
		err = manager.ConsumerGroups.CommitOffset(req.GroupID, req.TopicName, req.Partition, req.Offset, req.Metadata)
		if err != nil {
			log.Printf("Failed to commit offset: %v", err)
			return sendCommitOffsetError(conn, 2)
		}
	}

	return sendCommitOffsetResponse(conn, &CommitOffsetResponse{ErrorCode: 0})
}

// HandleFetchOffsetRequest 处理获取offset请求
func HandleFetchOffsetRequest(conn net.Conn, manager *metadata.Manager, clusterManager interface{}) error {
	req, err := parseFetchOffsetRequest(conn)
	if err != nil {
		return sendFetchOffsetError(conn, 1, -1)
	}

	// 注意：获取offset通常是只读操作，可以从本地读取
	// 但在集群模式下，可能需要确保数据一致性
	offset, exists := manager.ConsumerGroups.GetCommittedOffset(req.GroupID, req.TopicName, req.Partition)
	if !exists {
		return sendFetchOffsetError(conn, 3, -1) // Offset不存在
	}

	return sendFetchOffsetResponse(conn, &FetchOffsetResponse{
		ErrorCode: 0,
		Offset:    offset,
	})
}

// 解析请求的函数
func parseJoinGroupRequest(conn net.Conn) (*JoinGroupRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	// 读取GroupID
	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	// 读取ConsumerID
	var consumerIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &consumerIDLen); err != nil {
		return nil, err
	}
	consumerIDBytes := make([]byte, consumerIDLen)
	if _, err := io.ReadFull(conn, consumerIDBytes); err != nil {
		return nil, err
	}

	// 读取ClientID
	var clientIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &clientIDLen); err != nil {
		return nil, err
	}
	clientIDBytes := make([]byte, clientIDLen)
	if _, err := io.ReadFull(conn, clientIDBytes); err != nil {
		return nil, err
	}

	// 读取Topic数量
	var topicCount int32
	if err := binary.Read(conn, binary.BigEndian, &topicCount); err != nil {
		return nil, err
	}

	topics := make([]string, topicCount)
	for i := int32(0); i < topicCount; i++ {
		var topicLen int16
		if err := binary.Read(conn, binary.BigEndian, &topicLen); err != nil {
			return nil, err
		}
		topicBytes := make([]byte, topicLen)
		if _, err := io.ReadFull(conn, topicBytes); err != nil {
			return nil, err
		}
		topics[i] = string(topicBytes)
	}

	// 读取SessionTimeout
	var sessionTimeoutMs int32
	if err := binary.Read(conn, binary.BigEndian, &sessionTimeoutMs); err != nil {
		return nil, err
	}

	return &JoinGroupRequest{
		GroupID:        string(groupIDBytes),
		ConsumerID:     string(consumerIDBytes),
		ClientID:       string(clientIDBytes),
		Topics:         topics,
		SessionTimeout: time.Duration(sessionTimeoutMs) * time.Millisecond,
	}, nil
}

func parseLeaveGroupRequest(conn net.Conn) (*LeaveGroupRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	// 读取GroupID
	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	// 读取ConsumerID
	var consumerIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &consumerIDLen); err != nil {
		return nil, err
	}
	consumerIDBytes := make([]byte, consumerIDLen)
	if _, err := io.ReadFull(conn, consumerIDBytes); err != nil {
		return nil, err
	}

	return &LeaveGroupRequest{
		GroupID:    string(groupIDBytes),
		ConsumerID: string(consumerIDBytes),
	}, nil
}

func parseHeartbeatRequest(conn net.Conn) (*HeartbeatRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	// 读取GroupID
	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	// 读取ConsumerID
	var consumerIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &consumerIDLen); err != nil {
		return nil, err
	}
	consumerIDBytes := make([]byte, consumerIDLen)
	if _, err := io.ReadFull(conn, consumerIDBytes); err != nil {
		return nil, err
	}

	// 读取Generation
	var generation int32
	if err := binary.Read(conn, binary.BigEndian, &generation); err != nil {
		return nil, err
	}

	return &HeartbeatRequest{
		GroupID:    string(groupIDBytes),
		ConsumerID: string(consumerIDBytes),
		Generation: generation,
	}, nil
}

func parseCommitOffsetRequest(conn net.Conn) (*CommitOffsetRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	// 读取GroupID
	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	// 读取TopicName
	var topicLen int16
	if err := binary.Read(conn, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBytes); err != nil {
		return nil, err
	}

	// 读取Partition
	var partition int32
	if err := binary.Read(conn, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	// 读取Offset
	var offset int64
	if err := binary.Read(conn, binary.BigEndian, &offset); err != nil {
		return nil, err
	}

	// 读取Metadata
	var metadataLen int16
	if err := binary.Read(conn, binary.BigEndian, &metadataLen); err != nil {
		return nil, err
	}
	metadataBytes := make([]byte, metadataLen)
	if _, err := io.ReadFull(conn, metadataBytes); err != nil {
		return nil, err
	}

	return &CommitOffsetRequest{
		GroupID:   string(groupIDBytes),
		TopicName: string(topicBytes),
		Partition: partition,
		Offset:    offset,
		Metadata:  string(metadataBytes),
	}, nil
}

func parseFetchOffsetRequest(conn net.Conn) (*FetchOffsetRequest, error) {
	var version int16
	if err := binary.Read(conn, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	// 读取GroupID
	var groupIDLen int16
	if err := binary.Read(conn, binary.BigEndian, &groupIDLen); err != nil {
		return nil, err
	}
	groupIDBytes := make([]byte, groupIDLen)
	if _, err := io.ReadFull(conn, groupIDBytes); err != nil {
		return nil, err
	}

	// 读取TopicName
	var topicLen int16
	if err := binary.Read(conn, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topicBytes := make([]byte, topicLen)
	if _, err := io.ReadFull(conn, topicBytes); err != nil {
		return nil, err
	}

	// 读取Partition
	var partition int32
	if err := binary.Read(conn, binary.BigEndian, &partition); err != nil {
		return nil, err
	}

	return &FetchOffsetRequest{
		GroupID:   string(groupIDBytes),
		TopicName: string(topicBytes),
		Partition: partition,
	}, nil
}

// 发送响应的函数
func sendJoinGroupResponse(conn net.Conn, response *JoinGroupResponse) error {
	buf := new(bytes.Buffer)

	// ErrorCode
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	// Generation
	binary.Write(buf, binary.BigEndian, response.Generation)

	// GroupID
	binary.Write(buf, binary.BigEndian, int16(len(response.GroupID)))
	buf.WriteString(response.GroupID)

	// ConsumerID
	binary.Write(buf, binary.BigEndian, int16(len(response.ConsumerID)))
	buf.WriteString(response.ConsumerID)

	// Leader
	binary.Write(buf, binary.BigEndian, int16(len(response.Leader)))
	buf.WriteString(response.Leader)

	// Members count
	binary.Write(buf, binary.BigEndian, int32(len(response.Members)))
	for _, member := range response.Members {
		binary.Write(buf, binary.BigEndian, int16(len(member.ID)))
		buf.WriteString(member.ID)
		binary.Write(buf, binary.BigEndian, int16(len(member.ClientID)))
		buf.WriteString(member.ClientID)
	}

	// Assignment
	binary.Write(buf, binary.BigEndian, int32(len(response.Assignment)))
	for topic, partitions := range response.Assignment {
		binary.Write(buf, binary.BigEndian, int16(len(topic)))
		buf.WriteString(topic)
		binary.Write(buf, binary.BigEndian, int32(len(partitions)))
		for _, partition := range partitions {
			binary.Write(buf, binary.BigEndian, partition)
		}
	}

	// 发送响应长度和数据
	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendJoinGroupError(conn net.Conn, errorCode int16) error {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, errorCode)

	binary.Write(buf, binary.BigEndian, int32(0))
	binary.Write(buf, binary.BigEndian, int16(0))
	binary.Write(buf, binary.BigEndian, int16(0))
	binary.Write(buf, binary.BigEndian, int16(0))
	binary.Write(buf, binary.BigEndian, int32(0))

	// Assignment count
	binary.Write(buf, binary.BigEndian, int32(0)) // 没有分配

	responseData := buf.Bytes()
	if err := binary.Write(conn, binary.BigEndian, int32(len(responseData))); err != nil {
		return err
	}
	_, err := conn.Write(responseData)
	return err
}

func sendLeaveGroupResponse(conn net.Conn, response *LeaveGroupResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendLeaveGroupError(conn net.Conn, errorCode int16) error {
	return sendLeaveGroupResponse(conn, &LeaveGroupResponse{ErrorCode: errorCode})
}

func sendHeartbeatResponse(conn net.Conn, response *HeartbeatResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendHeartbeatError(conn net.Conn, errorCode int16) error {
	return sendHeartbeatResponse(conn, &HeartbeatResponse{ErrorCode: errorCode})
}

func sendCommitOffsetResponse(conn net.Conn, response *CommitOffsetResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendCommitOffsetError(conn net.Conn, errorCode int16) error {
	return sendCommitOffsetResponse(conn, &CommitOffsetResponse{ErrorCode: errorCode})
}

func sendFetchOffsetResponse(conn net.Conn, response *FetchOffsetResponse) error {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, response.ErrorCode)
	binary.Write(buf, binary.BigEndian, response.Offset)

	responseData := buf.Bytes()
	binary.Write(conn, binary.BigEndian, int32(len(responseData)))
	_, err := conn.Write(responseData)
	return err
}

func sendFetchOffsetError(conn net.Conn, errorCode int16, offset int64) error {
	return sendFetchOffsetResponse(conn, &FetchOffsetResponse{
		ErrorCode: errorCode,
		Offset:    offset,
	})
}
