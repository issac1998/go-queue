package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/issac1998/go-queue/internal/cluster"
)

// HandleClusterInfoRequest 处理集群信息查询请求
func HandleClusterInfoRequest(conn io.ReadWriter, clusterManager interface{}) error {
	// 如果集群管理器为nil，返回单机模式信息
	if clusterManager == nil {
		// 返回单机模式的集群信息
		singleNodeInfo := map[string]interface{}{
			"leader_id":    uint64(1),
			"leader_valid": true,
			"current_node": uint64(1),
			"is_leader":    true,
			"brokers":      make(map[string]interface{}),
			"topics":       make(map[string]interface{}),
		}

		responseData, err := json.Marshal(singleNodeInfo)
		if err != nil {
			log.Printf("Failed to marshal single node info: %v", err)
			return writeClusterInfoError(conn, "failed to serialize single node info")
		}

		return writeClusterInfoResponse(conn, responseData)
	}

	// 类型断言为集群管理器
	cm, ok := clusterManager.(interface {
		GetClusterInfo() (*cluster.ClusterInfo, error)
		IsLeader() bool
		GetLeaderID() (uint64, bool, error)
	})
	if !ok {
		return writeClusterInfoError(conn, "invalid cluster manager")
	}

	// 获取集群信息
	clusterInfo, err := cm.GetClusterInfo()
	if err != nil {
		log.Printf("Failed to get cluster info: %v", err)
		return writeClusterInfoError(conn, fmt.Sprintf("failed to get cluster info: %v", err))
	}

	// 序列化集群信息
	responseData, err := json.Marshal(clusterInfo)
	if err != nil {
		log.Printf("Failed to marshal cluster info: %v", err)
		return writeClusterInfoError(conn, "failed to serialize cluster info")
	}

	// 写入响应
	return writeClusterInfoResponse(conn, responseData)
}

// writeClusterInfoResponse 写入成功响应
func writeClusterInfoResponse(w io.Writer, data []byte) error {
	// 响应格式：
	// - 4 bytes: response length (包含error code)
	// - 2 bytes: error code (0 = success)
	// - N bytes: JSON data

	responseLen := int32(ErrorCodeSize + len(data)) // error code + data

	// 写入响应长度
	if err := binary.Write(w, binary.BigEndian, responseLen); err != nil {
		return fmt.Errorf("failed to write response length: %v", err)
	}

	// 写入成功错误码
	if err := binary.Write(w, binary.BigEndian, int16(0)); err != nil {
		return fmt.Errorf("failed to write error code: %v", err)
	}

	// 写入JSON数据
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("failed to write response data: %v", err)
	}

	return nil
}

// writeClusterInfoError 写入错误响应
func writeClusterInfoError(w io.Writer, errorMsg string) error {
	errorData := []byte(errorMsg)
	responseLen := int32(ErrorCodeSize + len(errorData)) // error code + error message

	// 写入响应长度
	if err := binary.Write(w, binary.BigEndian, responseLen); err != nil {
		return fmt.Errorf("failed to write error response length: %v", err)
	}

	// 写入错误码 (非零表示错误)
	if err := binary.Write(w, binary.BigEndian, int16(1)); err != nil {
		return fmt.Errorf("failed to write error code: %v", err)
	}

	// 写入错误消息
	if _, err := w.Write(errorData); err != nil {
		return fmt.Errorf("failed to write error message: %v", err)
	}

	return nil
}
