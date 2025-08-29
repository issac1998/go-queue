# BrokerAddrs 字段重构文档

## 概述

本次重构将原来的 `BrokerAddrs` 和 `BrokerAddr` 两个字段合并为一个统一的 `BrokerAddrs` 字段，使得API更加自然和简洁。

## 重构前后对比

### 重构前
```go
type ClientConfig struct {
    BrokerAddrs []string // 支持多个broker地址
    BrokerAddr  string   // 兼容单个地址的旧配置
    Timeout     time.Duration
}
```

### 重构后
```go
type ClientConfig struct {
    BrokerAddrs []string      // Broker地址列表，单个地址表示单Broker模式，多个地址表示集群模式
    Timeout     time.Duration
}
```

## 逻辑变更

### 模式判断逻辑
- **单Broker模式**: `len(BrokerAddrs) == 1`
- **集群模式**: `len(BrokerAddrs) > 1`
- **默认配置**: `len(BrokerAddrs) == 0` 时自动设置为 `["localhost:9092"]`

### 兼容性处理
原有的逻辑：
```go
if len(config.BrokerAddrs) > 0 {
    brokerAddrs = config.BrokerAddrs
} else if config.BrokerAddr != "" {
    brokerAddrs = []string{config.BrokerAddr}
} else {
    brokerAddrs = []string{"localhost:9092"}
}
```

新的逻辑：
```go
if len(config.BrokerAddrs) > 0 {
    brokerAddrs = config.BrokerAddrs
} else {
    brokerAddrs = []string{"localhost:9092"}
}
```

## 使用示例

### 单Broker模式
```go
client := client.NewClient(client.ClientConfig{
    BrokerAddrs: []string{"localhost:9092"},
    Timeout:     5 * time.Second,
})
```

### 集群模式
```go
client := client.NewClient(client.ClientConfig{
    BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
    Timeout:     10 * time.Second,
})
```

### 默认配置
```go
client := client.NewClient(client.ClientConfig{
    // 不设置 BrokerAddrs 将自动使用 ["localhost:9092"]
    Timeout: 5 * time.Second,
})
```

## 影响的文件

### 核心文件
- `client/client.go`: 主要的逻辑变更
- `client/client_test.go`: 测试文件更新

### 示例文件
- `examples/simple/main.go`
- `examples/consumer_groups/main.go`
- `examples/compression_dedup/main.go`
- `examples/message_replication/main.go`

### 测试文件
- `test/unified_client_test.go`
- `integration_test.go`

### 文档文件
- `README.md`
- `CLUSTER_CLIENT.md`
- `TEST_REPORT.md`
- `CONSUMER_GROUPS.md`

### 命令行工具
- `cmd/client/main.go`

### 脚本
- `scripts/regression_test.sh`

## 优势

1. **API统一性**: 只有一个字段来配置broker地址，更加简洁
2. **自然的语义**: 根据地址数量自动判断模式，无需额外配置
3. **向后兼容**: 现有代码只需简单修改即可适配
4. **减少歧义**: 避免了两个字段同时存在时的配置冲突

## 测试验证

重构后所有测试通过，验证了：
- 单Broker模式正确工作
- 集群模式正确工作  
- 默认配置正确处理
- 现有功能保持不变

## 迁移指南

如果您的代码中使用了旧的 `BrokerAddr` 字段，请按以下方式迁移：

```go
// 旧代码
ClientConfig{
    BrokerAddr: "localhost:9092",
    Timeout:    5 * time.Second,
}

// 新代码
ClientConfig{
    BrokerAddrs: []string{"localhost:9092"},
    Timeout:     5 * time.Second,
}
```

这个简单的改动就能完成迁移，不会影响任何功能。 