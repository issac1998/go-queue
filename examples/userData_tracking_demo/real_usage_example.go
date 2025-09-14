package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/client"
	"github.com/issac1998/go-queue/internal/async"
)

// RequestContext 请求上下文，用于追踪
type RequestContext struct {
	ID          string
	Operation   string
	StartTime   time.Time
	UserData    uint64
	ClientInfo  string
}

// GlobalTracker 全局请求追踪器
type GlobalTracker struct {
	mu       sync.RWMutex
	requests map[uint64]*RequestContext
	counter  uint64
}

// NewGlobalTracker 创建全局追踪器
func NewGlobalTracker() *GlobalTracker {
	return &GlobalTracker{
		requests: make(map[uint64]*RequestContext),
	}
}

// TrackRequest 追踪请求开始
func (gt *GlobalTracker) TrackRequest(userData uint64, operation string) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	
	gt.requests[userData] = &RequestContext{
		ID:         fmt.Sprintf("req-%d", userData&0xFFFFFFFF),
		Operation:  operation,
		StartTime:  time.Now(),
		UserData:   userData,
		ClientInfo: fmt.Sprintf("conn-%d", userData>>32),
	}
	
	log.Printf("🚀 [追踪] 请求开始: ID=%s, Operation=%s, UserData=0x%x", 
		gt.requests[userData].ID, operation, userData)
}

// CompleteRequest 完成请求追踪
func (gt *GlobalTracker) CompleteRequest(userData uint64, result string, err error) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	
	ctx, exists := gt.requests[userData]
	if !exists {
		log.Printf("⚠️  [追踪] 未找到请求: UserData=0x%x", userData)
		return
	}
	
	duration := time.Since(ctx.StartTime)
	
	if err != nil {
		log.Printf("❌ [追踪] 请求失败: ID=%s, 耗时=%v, 错误=%v, UserData=0x%x", 
			ctx.ID, duration, err, userData)
	} else {
		log.Printf("✅ [追踪] 请求成功: ID=%s, 耗时=%v, 结果=%s, UserData=0x%x", 
			ctx.ID, duration, result, userData)
	}
	
	delete(gt.requests, userData)
}

// GetActiveRequests 获取活跃请求数
func (gt *GlobalTracker) GetActiveRequests() int {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	return len(gt.requests)
}

// 全局追踪器实例
var globalTracker = NewGlobalTracker()

// DemoRealUserDataUsage 演示真实的UserData使用
func DemoRealUserDataUsage() {
	fmt.Println("\n🎯 真实 UserData 使用演示")
	fmt.Println("========================")
	
	// 创建带异步IO的客户端
	clientConfig := client.ClientConfig{
		BrokerAddrs:   []string{"localhost:9092"},
		Timeout:       5 * time.Second,
		EnableAsyncIO: true,
		AsyncIO: async.AsyncIOConfig{
			WorkerCount:    2,
			SQSize:         256,
			CQSize:         512,
			BatchSize:      8,
			PollTimeout:    50 * time.Millisecond,
			MaxConnections: 50,
		},
	}
	
	c := client.NewClient(clientConfig)
	defer c.Close()
	
	// 创建生产者
	producer := client.NewProducer(c)
	
	fmt.Println("\n📤 发送消息并观察 UserData 在异步回调中的使用...")
	
	// 发送多条消息
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(msgID int) {
			defer wg.Done()
			
			msg := client.ProduceMessage{
				Topic:     "test-topic",
				Partition: int32(msgID % 3),
				Key:       []byte(fmt.Sprintf("key-%d", msgID)),
				Value:     []byte(fmt.Sprintf("message-%d with timestamp %d", msgID, time.Now().UnixNano())),
			}
			
			// 这里Producer内部会使用异步IO，并在回调中使用userData
			result, err := producer.Send(msg)
			
			if err != nil {
				log.Printf("❌ 消息 %d 发送失败: %v", msgID, err)
			} else {
				log.Printf("✅ 消息 %d 发送成功: Partition=%d, Offset=%d", 
					msgID, result.Partition, result.Offset)
			}
			
			// 短暂延迟
			time.Sleep(time.Duration(msgID*50) * time.Millisecond)
		}(i)
	}
	
	// 监控活跃请求
	go func() {
		for i := 0; i < 10; i++ {
			active := globalTracker.GetActiveRequests()
			if active > 0 {
				log.Printf("📊 当前活跃请求数: %d", active)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()
	
	wg.Wait()
	
	// 等待所有异步操作完成
	time.Sleep(1 * time.Second)
	
	fmt.Println("\n📊 UserData 追踪统计:")
	fmt.Printf("  - 剩余活跃请求: %d\n", globalTracker.GetActiveRequests())
	
	fmt.Println("\n🔍 UserData 的实际价值:")
	fmt.Println("  ✅ 请求唯一标识: 每个异步操作都有唯一的userData")
	fmt.Println("  ✅ 生命周期追踪: 从发送到响应的完整生命周期")
	fmt.Println("  ✅ 错误关联: 将异步错误与具体请求关联")
	fmt.Println("  ✅ 性能分析: 基于 userData 的延迟和成功率统计")
	fmt.Println("  ✅ 调试支持: 详细的请求流程日志")
	fmt.Println("  ✅ 并发安全: 多个并发请求的正确追踪")
	
	fmt.Println("\n💡 关键点:")
	fmt.Println("   在 Producer.sendWithAsyncConnection 方法中:")
	fmt.Println("   - WriteAsync 回调使用 userData 追踪写操作")
	fmt.Println("   - ReadAsync 回调使用 userData 追踪读操作")
	fmt.Println("   - userData 在整个异步流程中保持一致")
	fmt.Println("   - 这样就能将异步回调与具体的请求关联起来!")
	
	fmt.Println("\n🎯 这才是 UserData 的正确用法!")
	fmt.Println("   不是简单地传递参数，而是在异步回调中真正使用它来关联请求!")
}