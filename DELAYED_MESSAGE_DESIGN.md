# 定时/延迟消息设计文档

## 概述

实现类似RocketMQ的定时消息功能，支持消息延迟投递到指定时间或延迟指定时长后投递。

## 功能需求

### 1. 核心功能
- **延迟时长**: 支持指定延迟多长时间后投递（如：延迟30秒）
- **定时投递**: 支持指定具体时间点投递（如：2024-09-22 15:00:00）
- **延迟级别**: 支持预设的延迟级别（1s, 5s, 10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h）
- **最大延迟**: 支持最大40天的延迟时间

### 2. 使用场景
- **订单超时处理**: 订单创建后30分钟未支付自动取消
- **定时推送**: 指定时间发送营销消息
- **重试机制**: 失败后延迟重试
- **定时任务**: 定时执行业务逻辑

## 架构设计

### 1. 存储架构
```
DelayedMessageStore
├── TimeWheel (时间轮调度器)
├── DelayedMessageIndex (延迟消息索引)
└── DelayedMessageData (延迟消息数据)
```

### 2. 核心组件

#### TimeWheel 时间轮
- **多级时间轮**: 支持秒级、分钟级、小时级、天级
- **高效调度**: O(1)时间复杂度的任务调度
- **内存友好**: 避免大量定时器对象

#### DelayedMessageManager
- **消息存储**: 管理延迟消息的持久化
- **定时调度**: 到期消息的投递调度
- **故障恢复**: 重启后恢复未投递的延迟消息

## 实现方案

### 1. 协议扩展
```go
// 延迟消息生产请求
type DelayedProduceRequest struct {
    Topic         string            `json:"topic"`
    Partition     int32             `json:"partition"`
    Key           []byte            `json:"key,omitempty"`
    Value         []byte            `json:"value"`
    Headers       map[string]string `json:"headers,omitempty"`
    DelayLevel    int32             `json:"delay_level,omitempty"`    // 延迟级别
    DelayTime     int64             `json:"delay_time,omitempty"`     // 延迟时长(毫秒)
    DeliverTime   int64             `json:"deliver_time,omitempty"`   // 投递时间戳
}
```

### 2. 客户端API
```go
// 延迟指定时长
producer.SendDelayed(message, 30*time.Second)

// 定时到指定时间
producer.SendAt(message, time.Date(2024, 9, 22, 15, 0, 0, 0, time.UTC))

// 使用延迟级别
producer.SendWithDelayLevel(message, DelayLevel30s)
```

### 3. 存储格式
```go
type DelayedMessage struct {
    ID            string            `json:"id"`
    Topic         string            `json:"topic"`
    Partition     int32             `json:"partition"`
    Key           []byte            `json:"key"`
    Value         []byte            `json:"value"`
    Headers       map[string]string `json:"headers"`
    CreateTime    int64             `json:"create_time"`
    DeliverTime   int64             `json:"deliver_time"`
    Status        DelayedMessageStatus `json:"status"`
    RetryCount    int               `json:"retry_count"`
}
```

## 实现步骤

### Phase 1: 基础框架
1. 定义延迟消息相关的数据结构
2. 实现时间轮调度器
3. 实现延迟消息存储

### Phase 2: 协议支持
1. 扩展网络协议支持延迟消息
2. 实现延迟消息的序列化/反序列化
3. 添加新的请求类型

### Phase 3: 客户端SDK
1. 扩展Producer支持延迟消息
2. 提供便捷的API接口
3. 添加延迟级别常量

### Phase 4: 测试验证
1. 单元测试
2. 集成测试
3. 性能测试

## 性能考虑

### 1. 内存使用
- 时间轮使用固定大小的槽位
- 延迟消息索引使用高效的数据结构
- 支持消息数据的延迟加载

### 2. 磁盘IO
- 批量写入延迟消息
- 定期清理已投递的消息
- 支持消息压缩

### 3. 调度精度
- 秒级调度精度
- 支持高并发的消息投递
- 避免消息投递的时间漂移

## 容错机制

### 1. 故障恢复
- 重启时自动加载未投递的延迟消息
- 支持消息状态的持久化
- 处理时钟调整的影响

### 2. 重试机制
- 投递失败时的重试策略
- 最大重试次数限制
- 重试间隔的指数退避

### 3. 监控告警
- 延迟消息积压监控
- 投递成功率监控
- 调度延迟监控 