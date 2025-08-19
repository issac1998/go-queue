# Consumer Groups 消费者组

Go Queue现在支持Consumer Groups功能！这是分布式消息队列的核心特性，允许多个消费者协同工作，自动分配分区，跟踪消费进度。

## 🚀 主要特性

- **自动分区分配**: 使用轮询算法自动分配分区给消费者
- **消费进度跟踪**: 自动跟踪和提交每个分区的消费offset
- **心跳机制**: 消费者定期发送心跳，检测故障和超时
- **动态重平衡**: 当消费者加入或离开时自动重新分配分区
- **故障恢复**: 消费者故障时自动重新分配其分区给其他消费者
- **Leader选举**: 自动选举组Leader负责分区分配协调

## 📦 架构组件

### 服务端组件
- **ConsumerGroupManager**: 管理所有消费者组的生命周期
- **OffsetStorage**: 存储和管理消费进度
- **分区分配算法**: 实现轮询分区分配策略
- **心跳检测**: 后台任务清理过期消费者

### 客户端组件
- **GroupConsumer**: 消费者组客户端API
- **自动心跳**: 后台线程维护与服务端的心跳
- **Offset管理**: 自动提交和获取消费进度

## 🔧 使用方法

### 1. 基本用法

```go
package main

import (
    "fmt"
    "log"
    "time"
    "github.com/issac1998/go-queue/client"
)

func main() {
    // 创建客户端
    c := client.NewClient(client.ClientConfig{
        BrokerAddr: "localhost:9092",
        Timeout:    5 * time.Second,
    })

    // 创建消费者组消费者
    groupConsumer := client.NewGroupConsumer(c, client.GroupConsumerConfig{
        GroupID:        "my-consumer-group",
        ConsumerID:     "consumer-1",
        Topics:         []string{"my-topic"},
        SessionTimeout: 30 * time.Second,
    })

    // 加入消费者组
    err := groupConsumer.JoinGroup()
    if err != nil {
        log.Fatalf("Failed to join group: %v", err)
    }

    // 查看分区分配
    assignment := groupConsumer.GetAssignment()
    fmt.Printf("Assigned partitions: %v\n", assignment)

    // 消费消息（实际使用中需要实现完整的消费循环）
    // 见完整示例 examples/consumer_groups/main.go

    // 离开消费者组
    defer groupConsumer.LeaveGroup()
}
```

### 2. 多消费者协同工作

```go
// 创建多个消费者
consumers := make([]*client.GroupConsumer, 3)

for i := 0; i < 3; i++ {
    consumerID := fmt.Sprintf("consumer-%d", i+1)
    
    consumers[i] = client.NewGroupConsumer(c, client.GroupConsumerConfig{
        GroupID:        "my-group",
        ConsumerID:     consumerID,
        Topics:         []string{"my-topic"},
        SessionTimeout: 30 * time.Second,
    })
    
    // 加入组
    consumers[i].JoinGroup()
    
    // 查看分配的分区
    assignment := consumers[i].GetAssignment()
    fmt.Printf("Consumer %s assignment: %v\n", consumerID, assignment)
}
```

### 3. Offset管理

```go
// 提交offset
err := groupConsumer.CommitOffset("my-topic", 0, 100, "processed successfully")
if err != nil {
    log.Printf("Failed to commit offset: %v", err)
}

// 获取已提交的offset
offset, err := groupConsumer.FetchCommittedOffset("my-topic", 0)
if err != nil {
    log.Printf("Failed to fetch offset: %v", err)
} else {
    fmt.Printf("Last committed offset: %d\n", offset)
}
```

## 🎯 完整示例

运行完整的Consumer Groups演示：

```bash
# 启动broker
go run cmd/broker/main.go

# 在另一个终端运行Consumer Groups示例
go run examples/consumer_groups/main.go
```

## 📖 API 参考

### GroupConsumer

```go
type GroupConsumer struct {
    client         *Client
    GroupID        string
    ConsumerID     string
    Topics         []string
    SessionTimeout time.Duration
}

type GroupConsumerConfig struct {
    GroupID        string        // 消费者组ID
    ConsumerID     string        // 消费者ID（组内唯一）
    Topics         []string      // 订阅的Topic列表
    SessionTimeout time.Duration // 会话超时时间
}

// 创建新的消费者组消费者
func NewGroupConsumer(client *Client, config GroupConsumerConfig) *GroupConsumer

// 加入消费者组
func (gc *GroupConsumer) JoinGroup() error

// 离开消费者组
func (gc *GroupConsumer) LeaveGroup() error

// 提交offset
func (gc *GroupConsumer) CommitOffset(topic string, partition int32, offset int64, metadata string) error

// 获取已提交的offset
func (gc *GroupConsumer) FetchCommittedOffset(topic string, partition int32) (int64, error)

// 获取分区分配
func (gc *GroupConsumer) GetAssignment() map[string][]int32
```

## 🔮 工作原理

### 1. 消费者组生命周期

1. **创建组**: 第一个消费者加入时创建消费者组
2. **加入组**: 消费者发送JoinGroup请求
3. **Leader选举**: 第一个加入的消费者成为Leader
4. **分区分配**: Leader负责分配分区给所有消费者
5. **稳定状态**: 所有消费者开始消费分配的分区
6. **重平衡**: 当消费者加入/离开时触发重新分配

### 2. 分区分配算法

当前实现使用**轮询分配**算法：

```
消费者: [consumer-1, consumer-2, consumer-3]
分区:   [partition-0, partition-1, partition-2, partition-3]

分配结果:
- consumer-1: [partition-0, partition-3]
- consumer-2: [partition-1]
- consumer-3: [partition-2]
```

### 3. 心跳和故障检测

- 消费者每`SessionTimeout/3`发送一次心跳
- 服务端每30秒检查一次过期消费者
- 超过`SessionTimeout`没有心跳的消费者会被移除
- 移除消费者后自动触发重平衡

### 4. Offset管理

- 每个消费者组维护独立的offset存储
- Offset按`GroupID + Topic + Partition`存储
- 支持手动提交offset
- 消费者可以从上次提交的位置继续消费

## ⚡ 性能特性

- **并发安全**: 所有操作都是线程安全的
- **内存效率**: 使用读写锁减少锁竞争
- **网络优化**: 最小化网络交互次数
- **故障恢复快**: 30秒内检测和处理消费者故障

## 🛡️ 错误处理

### 常见错误码

- `1`: 协议解析错误
- `2`: 服务器内部错误
- `3`: 消费者组/Topic不存在

### 最佳实践

1. **合理设置SessionTimeout**: 建议30-60秒
2. **及时处理错误**: 检查所有API调用的返回错误
3. **优雅关闭**: 应用退出前调用LeaveGroup()
4. **分区数量**: 分区数应该 >= 消费者数量以获得最佳性能

## 🔄 下一步计划

- [ ] 支持更多分区分配策略（Range, Sticky等）
- [ ] 添加消费者组管理API（ListGroups, DescribeGroup等）
- [ ] 支持自动offset提交
- [ ] 添加消费lag监控
- [ ] 支持消费者组重置offset

## 🤝 贡献

欢迎提交Issue和Pull Request来改进Consumer Groups功能！

特别欢迎以下贡献：
- 新的分区分配算法
- 性能优化
- 错误处理改进
- 文档和示例完善 