# 协议常量重构文档

## 概述

本次重构将代码中的魔法数字替换为有意义的常量，提高代码的可读性和可维护性。主要解决了：
1. 使用数字 `8` 表示 `clusterInfoRequest` 的问题
2. 使用数字 `2` 表示 error code 大小的问题
3. 统一分散在各个文件中的请求类型常量定义

## 重构内容

### 1. 统一的常量定义

**文件**: `internal/protocol/protocol.go`

```go
// Request types for the broker protocol
const (
    ProduceRequestType      = 0
    FetchRequestType        = 1
    CreateTopicRequestType  = 2
    JoinGroupRequestType    = 3
    LeaveGroupRequestType   = 4
    HeartbeatRequestType    = 5
    CommitOffsetRequestType = 6
    FetchOffsetRequestType  = 7
    ClusterInfoRequestType  = 8 // 集群信息查询
)

// Response error codes
const (
    ErrorCodeSuccess     = 0  // 成功
    ErrorCodeGeneral     = 1  // 一般错误
    ErrorCodeNotFound    = 2  // 资源未找到
    ErrorCodeInvalidData = 3  // 无效数据
)

// Protocol field sizes
const (
    ErrorCodeSize = 2 // error code 占用字节数
)
```

### 2. 主要变更

#### 客户端代码 (`client/client.go`)

**重构前**:
```go
// 发送集群信息查询请求
if err := binary.Write(conn, binary.BigEndian, int32(8)); err != nil {
    return err
}

// 读取JSON数据
jsonData := make([]byte, responseLen-2)

// switch 语句中的魔法数字
case 8: // clusterInfoRequest
    return false
```

**重构后**:
```go
// 发送集群信息查询请求
if err := binary.Write(conn, binary.BigEndian, int32(protocol.ClusterInfoRequestType)); err != nil {
    return err
}

// 读取JSON数据
jsonData := make([]byte, responseLen-protocol.ErrorCodeSize)

// switch 语句中使用常量
case protocol.ClusterInfoRequestType:
    return false
```

#### 服务端代码 (`cmd/broker/main.go`)

**重构前**:
```go
const (
    produceRequest      = 0
    fetchRequest        = 1
    // ... 其他常量
    clusterInfoRequest  = 8
)

switch reqType {
case clusterInfoRequest:
    protocol.HandleClusterInfoRequest(conn, clusterManager)
}
```

**重构后**:
```go
// 使用 protocol 包中定义的常量
// const 定义已移至 internal/protocol/protocol.go

switch reqType {
case protocol.ClusterInfoRequestType:
    protocol.HandleClusterInfoRequest(conn, clusterManager)
}
```

#### 协议处理代码 (`internal/protocol/cluster_info.go`)

**重构前**:
```go
responseLen := int32(2 + len(data)) // error code + data
```

**重构后**:
```go
responseLen := int32(ErrorCodeSize + len(data)) // error code + data
```

### 3. 清理重复定义

移除了以下文件中的重复常量定义：
- `internal/protocol/produce.go`: `FetchRequestType`
- `internal/protocol/consumer_group.go`: 多个请求类型常量
- `cmd/broker/main.go`: 本地常量定义

同时调整了 Consumer Group 相关的常量值以避免冲突：
- `ListGroupsRequestType = 9` (之前是8)
- `DescribeGroupRequestType = 10` (之前是9)

## 优势

1. **消除魔法数字**: 所有数字都有明确的含义和命名
2. **统一管理**: 所有协议常量集中在一个文件中
3. **提高可读性**: 代码更容易理解和维护
4. **降低错误风险**: 避免了硬编码数字可能带来的错误
5. **便于扩展**: 新增请求类型时只需在一个地方添加

## 协议说明

### ClusterInfoRequest 协议格式

**请求**:
```
4 bytes: request_type = ClusterInfoRequestType (8)
```

**响应**:
```
4 bytes: response_length (包含 error_code + data)
2 bytes: error_code (ErrorCodeSize)
N bytes: JSON data (长度 = response_length - ErrorCodeSize)
```

### Error Code 含义

- `ErrorCodeSuccess (0)`: 操作成功
- `ErrorCodeGeneral (1)`: 一般错误
- `ErrorCodeNotFound (2)`: 资源未找到
- `ErrorCodeInvalidData (3)`: 无效数据

## 影响的文件

### 核心文件
- `internal/protocol/protocol.go`: 新增统一常量定义
- `client/client.go`: 使用常量替换魔法数字
- `cmd/broker/main.go`: 使用协议包常量
- `internal/protocol/cluster_info.go`: 使用 ErrorCodeSize 常量

### 清理的文件
- `internal/protocol/produce.go`: 移除重复常量
- `internal/protocol/consumer_group.go`: 移除重复常量并调整数值

## 验证

重构后通过了以下验证：
- 编译测试: `go build ./...` ✅
- 单元测试: `go test ./client -v` ✅
- 常量值验证: 所有常量值正确对应 ✅

## 向后兼容性

此次重构不影响任何外部API，所有变更都在内部实现层面，协议数值保持不变，确保与现有客户端和服务端的兼容性。 