package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/issac1998/go-queue/client"
)

// MessageTracker 消息追踪器
type MessageTracker struct {
	mu           sync.RWMutex
	pendingMsgs  map[uint64]*TrackedMessage
	totalSent    int64
	totalSuccess int64
	totalFailed  int64
	minLatency   time.Duration
	maxLatency   time.Duration
	avgLatency   time.Duration
}

// TrackedMessage 被追踪的消息
type TrackedMessage struct {
	ID        uint64
	Topic     string
	Payload   []byte
	SentTime  time.Time
	Retries   int
	UserData  uint64
}

// NewMessageTracker 创建消息追踪器
func NewMessageTracker() *MessageTracker {
	return &MessageTracker{
		pendingMsgs: make(map[uint64]*TrackedMessage),
		minLatency:  time.Hour, // 初始化为很大的值
	}
}

// TrackMessage 开始追踪消息
func (mt *MessageTracker) TrackMessage(msgID uint64, topic string, payload []byte, userData uint64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.pendingMsgs[msgID] = &TrackedMessage{
		ID:       msgID,
		Topic:    topic,
		Payload:  payload,
		SentTime: time.Now(),
		UserData: userData,
	}

	atomic.AddInt64(&mt.totalSent, 1)
	log.Printf("📤 [SEND] 消息发送: msgID=%d, userData=%d, topic=%s, size=%d bytes",
		msgID, userData, topic, len(payload))
}

// CompleteMessage 完成消息追踪
func (mt *MessageTracker) CompleteMessage(userData uint64, success bool, errorMsg string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// 通过 userData 查找对应的消息
	var msgID uint64
	var trackedMsg *TrackedMessage
	for id, msg := range mt.pendingMsgs {
		if msg.UserData == userData {
			msgID = id
			trackedMsg = msg
			break
		}
	}

	if trackedMsg == nil {
		log.Printf("⚠️ [WARN] 未找到对应消息: userData=%d", userData)
		return
	}

	latency := time.Since(trackedMsg.SentTime)
	delete(mt.pendingMsgs, msgID)

	// 更新统计信息
	if success {
		atomic.AddInt64(&mt.totalSuccess, 1)
		log.Printf("✅ [SUCCESS] 消息成功: msgID=%d, userData=%d, 延迟=%v, topic=%s",
			msgID, userData, latency, trackedMsg.Topic)
	} else {
		atomic.AddInt64(&mt.totalFailed, 1)
		log.Printf("❌ [FAILED] 消息失败: msgID=%d, userData=%d, 延迟=%v, 错误=%s",
			msgID, userData, latency, errorMsg)
	}

	// 更新延迟统计
	if latency < mt.minLatency {
		mt.minLatency = latency
	}
	if latency > mt.maxLatency {
		mt.maxLatency = latency
	}

	// 计算平均延迟
	total := atomic.LoadInt64(&mt.totalSuccess) + atomic.LoadInt64(&mt.totalFailed)
	if total > 0 {
		mt.avgLatency = (mt.avgLatency*time.Duration(total-1) + latency) / time.Duration(total)
	}
}

// GetStats 获取统计信息
func (mt *MessageTracker) GetStats() (sent, success, failed int64, pending int, minLat, maxLat, avgLat time.Duration) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return atomic.LoadInt64(&mt.totalSent),
		atomic.LoadInt64(&mt.totalSuccess),
		atomic.LoadInt64(&mt.totalFailed),
		len(mt.pendingMsgs),
		mt.minLatency,
		mt.maxLatency,
		mt.avgLatency
}

// DemoRealWorldTracking 演示真实世界的追踪应用
func DemoRealWorldTracking() {
	fmt.Println("\n🌍 真实世界 UserData 追踪演示")
	fmt.Println("==============================")

	// 创建消息追踪器
	tracker := NewMessageTracker()

	// 创建客户端配置
	clientConfig := client.ClientConfig{
		BrokerAddrs: []string{"localhost:9092"},
		Timeout:     5 * time.Second,
	}

	// 创建客户端
	c := client.NewClient(clientConfig)
	defer c.Close()

	fmt.Println("\n📊 开始发送消息并追踪...")

	// 模拟发送多条消息
	var wg sync.WaitGroup
	msgIDCounter := uint64(1000)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// 生成消息ID和userData
			msgID := atomic.AddUint64(&msgIDCounter, 1)
			connID := int64(index%3 + 1) // 模拟3个连接
			timestamp := time.Now().UnixNano() & 0xFFFFFFFF
			userData := uint64(connID)<<32 | uint64(timestamp)

			// 创建消息
			topic := fmt.Sprintf("test-topic-%d", index%3)
			payload := []byte(fmt.Sprintf("消息内容 %d - 时间戳: %d", msgID, time.Now().Unix()))

			msg := client.ProduceMessage{
				Topic:     topic,
				Partition: int32(index % 3),
				Value:     payload,
			}

			// 开始追踪
			tracker.TrackMessage(msgID, topic, payload, userData)

			// 发送消息
			producer := client.NewProducer(c)
			_, err := producer.Send(msg)

			// 模拟异步回调
			go func(ud uint64, success bool, errMsg string) {
				// 模拟网络延迟
				time.Sleep(time.Duration(10+index*5) * time.Millisecond)
				tracker.CompleteMessage(ud, success, errMsg)
			}(userData, err == nil, func() string {
				if err != nil {
					return err.Error()
				}
				return ""
			}())

			// 模拟发送间隔
			time.Sleep(time.Duration(20+index*10) * time.Millisecond)
		}(i)
	}

	// 启动统计监控
	go func() {
		for i := 0; i < 15; i++ {
			sent, success, failed, pending, minLat, maxLat, avgLat := tracker.GetStats()
			log.Printf("📈 [STATS] 发送:%d, 成功:%d, 失败:%d, 待处理:%d, 延迟(min/avg/max): %v/%v/%v",
				sent, success, failed, pending, minLat, avgLat, maxLat)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	wg.Wait()
	time.Sleep(200 * time.Millisecond) // 等待所有回调完成

	// 最终统计
	sent, success, failed, pending, minLat, maxLat, avgLat := tracker.GetStats()

	fmt.Println("\n📊 最终统计结果:")
	fmt.Printf("  📤 总发送: %d 条消息\n", sent)
	fmt.Printf("  ✅ 成功: %d 条 (%.1f%%)\n", success, float64(success)/float64(sent)*100)
	fmt.Printf("  ❌ 失败: %d 条 (%.1f%%)\n", failed, float64(failed)/float64(sent)*100)
	fmt.Printf("  ⏳ 待处理: %d 条\n", pending)
	fmt.Printf("  🕐 延迟统计: 最小=%v, 平均=%v, 最大=%v\n", minLat, avgLat, maxLat)

	fmt.Println("\n🎯 UserData 在消息队列中的价值:")
	fmt.Println("  ✅ 端到端追踪: 从发送到确认的完整链路")
	fmt.Println("  ✅ 性能监控: 延迟、吞吐量、成功率统计")
	fmt.Println("  ✅ 故障诊断: 快速定位失败的消息和原因")
	fmt.Println("  ✅ 负载均衡: 基于连接ID的请求分发")
	fmt.Println("  ✅ 重试机制: 基于userData的幂等性保证")
	fmt.Println("  ✅ 监控告警: 实时的系统健康状态")

	fmt.Println("\n✅ 真实世界追踪演示完成!")
}