# 统一智能客户端

## 🎯 设计原理

**统一入口，自动检测** - 无需区分集群和单机客户端，基于Raft算法特性的智能路由：

### 核心原则
- **统一接口** - 同一个`Client`支持单机和集群模式
- **自动检测** - 启动时自动检测集群模式并配置路由策略  
- **智能路由** - 写操作→Leader，读操作→Follower
- **故障自愈** - 自动检测节点故障并重新路由

## 🏗️ 架构设计

```
Client Request
      ↓
┌─────────────────┐
│  Unified Client │ ← 统一入口
├─────────────────┤
│ 自动检测模式     │
│ ┌─────┬─────┐   │
│ │单机 │集群 │   │ 
│ │模式 │模式 │   │
│ └─────┴─────┘   │
└─────────────────┘
      ↓
┌─────────────────┐
│ 智能路由层      │
├─────────────────┤
│ 写操作 → Leader │
│ 读操作 → Follower│ 
└─────────────────┘
      ↓
┌─────────────────┐
│ Broker Cluster  │
└─────────────────┘
```

## 🔧 服务端支持

### 1. Leader检测API
使用dragonboat内置的Leader检测功能：
```go
// 在cluster/manager.go中
func (cm *Manager) IsLeader() bool {
    leaderID, valid, err := cm.nodeHost.GetLeaderID(ClusterID)
    return err == nil && valid && leaderID == cm.nodeID
}
```

### 2. 集群信息查询协议
新增协议类型 `clusterInfoRequest = 8`，返回：
```json
{
  "leader_id": 1,
  "leader_valid": true,
  "current_node": 1,
  "is_leader": true,
  "brokers": {...},
  "topics": {...}
}
```

### 3. Follower读取支持
利用dragonboat的ReadIndex：
```go
// 强一致性读（所有节点）
result, err := nodeHost.ReadIndex(clusterID, timeout)


## 💡 客户端使用

### 统一接口（自动检测模式）
```go
// 单机模式
client := client.NewClient(client.ClientConfig{
            BrokerAddrs: []string{"localhost:9092"},
    Timeout:    5 * time.Second,
})

// 集群模式（自动检测并启用负载均衡）
client := client.NewClient(client.ClientConfig{
    BrokerAddrs: []string{"localhost:9092", "localhost:9093", "localhost:9094"},
    Timeout:     10 * time.Second,
})

// 统一的API调用（自动智能路由）
producer := client.NewProducer(client)
consumer := client.NewConsumer(client)

// 写操作自动路由到Leader，读操作自动负载均衡到Follower
```

### 自动故障处理
- Leader故障 → 自动重新发现Leader
- 节点故障 → 自动切换到其他健康节点
- 网络异常 → 标记节点不健康，避免重复尝试

## 🎯 关键优势

1. **统一接口**: 同一套API支持单机和集群，无需代码改动
2. **自动检测**: 启动时自动识别集群模式，无需手动配置
3. **智能路由**: 写操作精准路由到Leader，读操作负载均衡到Follower  
4. **故障自愈**: 自动检测节点故障并重新路由
5. **向后兼容**: 完全兼容原有的单机客户端代码

## 📊 与传统方案对比

| 特性 | 传统客户端 | 统一智能客户端 |
|------|------------|---------------|
| 接口复杂度 | ❌ 需要区分单机/集群 | ✅ 统一接口自动检测 |
| 写操作效率 | ❌ 大量重定向 | ✅ 直接命中Leader |
| 读操作性能 | ⚠️ 所有请求打Leader | ✅ 分散到Follower |
| 故障处理 | ❌ 简单重试 | ✅ 智能切换 |
| 代码迁移 | ❌ 需要大量改动 | ✅ 完全兼容 |

## 🚀 实现状态

### ✅ 已完成
- [x] 统一客户端接口
- [x] 自动集群模式检测
- [x] 智能读写路由
- [x] Leader检测和切换
- [x] Follower负载均衡
- [x] 向后兼容性

### 🔮 后续优化
- [ ] 连接池复用
- [ ] 更精确的延迟统计
- [ ] 分区级别的Leader映射
- [ ] 健康检查优化

**完美解决方案** - 统一接口，自动检测，智能路由，向后兼容！ 🎯 