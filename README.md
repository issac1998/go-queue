# Go Queue 分布式消息队列

一个用Go语言实现的高性能分布式消息队列系统，提供类似Apache Kafka的功能。

## 🚀 特性

- **高性能消息存储**: Segment-based存储架构，支持快速读写
- **分区支持**: 每个Topic可包含多个分区，提供水平扩展能力
- **持久化**: 消息数据持久化到磁盘，保证数据不丢失
- **客户端SDK**: 提供易用的Go客户端库
- **TCP协议**: 自定义二进制协议，保证高性能通信

## 📦 项目结构

```
go-queue/
├── cmd/
│   ├── broker/          # 消息队列服务端
│   └── client/          # 命令行客户端工具
├── pkg/
│   └── client/            # 客户端SDK
├── internal/
│   ├── metadata/       # 元数据管理
│   ├── protocol/       # 网络协议
│   └── storage/        # 存储引擎
└── examples/           # 使用示例
```

## 🔧 安装和使用

### 1. 启动服务端

```bash
# 编译并启动服务端
cd cmd/broker
go build -o broker
./broker

# 或者直接运行
go run cmd/broker/main.go
```

服务端将在 `localhost:9092` 启动。

### 2. 使用客户端SDK

#### 基本使用示例

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/issac1998/go-queue/pkg/client"
)

func main() {
    // 创建客户端
    client := client.NewClient(client.ClientConfig{
        BrokerAddr: "localhost:9092",
        Timeout:    5 * time.Second,
    })

    // 创建管理员客户端
    admin := client.NewAdmin(client)
    
    // 创建主题
    result, err := admin.CreateTopic(client.CreateTopicRequest{
        Name:       "my-topic",
        Partitions: 3,
        Replicas:   1,
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("主题创建成功: %s\n", result.Name)

    // 创建生产者
    producer := client.NewProducer(client)
    
    // 发送消息
    msg := client.ProduceMessage{
        Topic:     "my-topic",
        Partition: 0,
        Value:     []byte("Hello, Go Queue!"),
    }
    
    sendResult, err := producer.Send(msg)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("消息发送成功，Offset: %d\n", sendResult.Offset)

    // 创建消费者
    consumer := client.NewConsumer(client)
    
    // 拉取消息
    fetchResult, err := consumer.FetchFrom("my-topic", 0, 0)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, msg := range fetchResult.Messages {
        fmt.Printf("收到消息: %s\n", string(msg.Value))
    }
}
```

#### 批量发送消息

```go
producer := client.NewProducer(client)

messages := []client.ProduceMessage{
    {Topic: "my-topic", Partition: 0, Value: []byte("消息1")},
    {Topic: "my-topic", Partition: 0, Value: []byte("消息2")},
    {Topic: "my-topic", Partition: 0, Value: []byte("消息3")},
}

result, err := producer.SendBatch(messages)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("批量发送成功，起始Offset: %d\n", result.Offset)
```

#### 订阅消息

```go
consumer := client.NewConsumer(client)

// 定义消息处理函数
handler := func(msg client.Message) error {
    fmt.Printf("处理消息: %s, Offset: %d\n", string(msg.Value), msg.Offset)
    return nil
}

// 订阅主题（阻塞式）
err := consumer.Subscribe("my-topic", 0, handler)
if err != nil {
    log.Fatal(err)
}
```

### 3. 使用命令行工具

```bash
cd cmd/client

# 创建主题
go run main.go -cmd=create-topic -topic=test-topic

# 发送单条消息
go run main.go -cmd=produce -topic=test-topic -partition=0 -message="Hello World"

# 批量发送消息
go run main.go -cmd=produce -topic=test-topic -partition=0 -message="Test" -count=5

# 消费消息
go run main.go -cmd=consume -topic=test-topic -partition=0 -offset=0
```

## 📖 API 文档

### Client

```go
type ClientConfig struct {
    BrokerAddr string        // Broker地址，默认localhost:9092
    Timeout    time.Duration // 连接超时时间，默认5秒
}

func NewClient(config ClientConfig) *Client
```

### Producer

```go
type Producer struct {
    client *Client
}

type ProduceMessage struct {
    Topic     string
    Partition int32
    Value     []byte
}

type ProduceResult struct {
    Topic     string
    Partition int32
    Offset    int64
    Error     error
}

func NewProducer(client *Client) *Producer
func (p *Producer) Send(msg ProduceMessage) (*ProduceResult, error)
func (p *Producer) SendBatch(messages []ProduceMessage) (*ProduceResult, error)
```

### Consumer

```go
type Consumer struct {
    client *Client
}

type Message struct {
    Topic     string
    Partition int32
    Offset    int64
    Value     []byte
}

type FetchResult struct {
    Topic      string
    Partition  int32
    Messages   []Message
    NextOffset int64
    Error      error
}

func NewConsumer(client *Client) *Consumer
func (c *Consumer) FetchFrom(topic string, partition int32, offset int64) (*FetchResult, error)
func (c *Consumer) Subscribe(topic string, partition int32, handler func(Message) error) error
```

### Admin

```go
type Admin struct {
    client *Client
}

type CreateTopicRequest struct {
    Name       string
    Partitions int32
    Replicas   int32
}

type CreateTopicResult struct {
    Name  string
    Error error
}

func NewAdmin(client *Client) *Admin
func (a *Admin) CreateTopic(req CreateTopicRequest) (*CreateTopicResult, error)
```

## 🎯 完整示例

查看 `examples/simple/main.go` 获取完整的使用示例。

```bash
# 运行示例
go run examples/simple/main.go
```

## 🔮 架构说明

### 存储架构
- **Segment**: 消息数据按Segment分片存储，每个Segment包含数据文件和索引文件
- **Partition**: 每个Topic可以有多个分区，分区内消息有序
- **Index**: 支持Offset索引和时间索引，实现快速查找

### 网络协议
- **TCP连接**: 客户端与服务端通过TCP连接通信
- **二进制协议**: 自定义二进制协议，支持版本控制
- **请求类型**: 支持Produce、Fetch、CreateTopic等请求类型

## 🚧 开发状态

当前实现的功能：
- ✅ 基础的生产者/消费者功能
- ✅ Topic和Partition管理
- ✅ 持久化存储
- ✅ 客户端SDK
- ✅ 命令行工具

计划实现的功能：
- ⏳ 消费者组（Consumer Group）
- ⏳ 多Broker集群支持
- ⏳ 数据副本和故障恢复
- ⏳ 消息压缩
- ⏳ HTTP API接口
- ⏳ 监控和度量指标

## 🤝 贡献

欢迎提交Issue和Pull Request来改进项目！

## �� 许可证

MIT License
