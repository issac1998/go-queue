package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/async"
)

// RequestTracker 用于追踪异步请求
type RequestTracker struct {
	mu       sync.RWMutex
	requests map[uint64]*RequestInfo
}

// RequestInfo 存储请求信息
type RequestInfo struct {
	ID        uint64
	Operation string
	StartTime time.Time
	ConnID    int64
	Timestamp int64
}

// NewRequestTracker 创建请求追踪器
func NewRequestTracker() *RequestTracker {
	return &RequestTracker{
		requests: make(map[uint64]*RequestInfo),
	}
}

// TrackRequest 追踪请求
func (rt *RequestTracker) TrackRequest(userData uint64, operation string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// 解析 userData: 高32位是连接ID，低32位是时间戳
	connID := int64(userData >> 32)
	timestamp := int64(userData & 0xFFFFFFFF)

	rt.requests[userData] = &RequestInfo{
		ID:        userData,
		Operation: operation,
		StartTime: time.Now(),
		ConnID:    connID,
		Timestamp: timestamp,
	}

	log.Printf("🔍 [TRACK] 请求开始: userData=%d, connID=%d, op=%s", userData, connID, operation)
}

// CompleteRequest 完成请求追踪
func (rt *RequestTracker) CompleteRequest(userData uint64, result string, err error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	req, exists := rt.requests[userData]
	if !exists {
		log.Printf("⚠️ [TRACK] 未找到请求: userData=%d", userData)
		return
	}

	duration := time.Since(req.StartTime)
	delete(rt.requests, userData)

	if err != nil {
		log.Printf("❌ [TRACK] 请求失败: userData=%d, connID=%d, op=%s, 耗时=%v, 错误=%v",
			userData, req.ConnID, req.Operation, duration, err)
	} else {
		log.Printf("✅ [TRACK] 请求成功: userData=%d, connID=%d, op=%s, 耗时=%v, 结果=%s",
			userData, req.ConnID, req.Operation, duration, result)
	}
}

// GetActiveRequests 获取活跃请求数
func (rt *RequestTracker) GetActiveRequests() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return len(rt.requests)
}

// DemoAdvancedTracking 演示高级追踪功能
func DemoAdvancedTracking() {
	fmt.Println("\n🚀 高级 UserData 追踪演示")
	fmt.Println("========================")

	// 创建追踪器
	tracker := NewRequestTracker()

	// 创建异步IO
	config := async.AsyncIOConfig{
		WorkerCount:    2,
		SQSize:         256,
		CQSize:         512,
		BatchSize:      8,
		PollTimeout:    50 * time.Millisecond,
		MaxConnections: 50,
	}

	asyncIO := async.NewAsyncIO(config)
	if err := asyncIO.Start(); err != nil {
		log.Printf("❌ 启动异步IO失败: %v", err)
		return
	}
	defer asyncIO.Close()

	fmt.Println("\n📡 模拟异步操作...")

	// 模拟多个并发异步操作
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 模拟创建连接和异步操作
			// 在实际应用中，这里会是真实的网络连接
			connID := int64(id + 1)
			timestamp := time.Now().UnixNano() & 0xFFFFFFFF
			userData := uint64(connID)<<32 | uint64(timestamp)

			// 追踪请求开始
			tracker.TrackRequest(userData, fmt.Sprintf("async-op-%d", id))

			// 模拟异步操作延迟
			time.Sleep(time.Duration(50+id*20) * time.Millisecond)

			// 模拟操作完成
			if id%3 == 0 {
				// 模拟失败
				tracker.CompleteRequest(userData, "", fmt.Errorf("模拟错误 %d", id))
			} else {
				// 模拟成功
				tracker.CompleteRequest(userData, fmt.Sprintf("成功结果-%d", id), nil)
			}
		}(i)
	}

	// 监控活跃请求
	go func() {
		for i := 0; i < 10; i++ {
			active := tracker.GetActiveRequests()
			log.Printf("📊 [MONITOR] 活跃请求数: %d", active)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // 等待监控完成

	fmt.Println("\n📋 追踪功能说明:")
	fmt.Println("  ✅ 请求唯一标识: userData = (连接ID << 32) | 时间戳")
	fmt.Println("  ✅ 请求生命周期追踪: 开始时间、持续时间、结果")
	fmt.Println("  ✅ 并发安全: 使用读写锁保护共享状态")
	fmt.Println("  ✅ 错误处理: 区分成功和失败的请求")
	fmt.Println("  ✅ 实时监控: 活跃请求数量统计")
	fmt.Println("  ✅ 调试支持: 详细的日志记录")

	fmt.Println("\n🎯 实际应用场景:")
	fmt.Println("  - 分布式追踪系统集成")
	fmt.Println("  - 性能监控和指标收集")
	fmt.Println("  - 请求去重和幂等性保证")
	fmt.Println("  - 超时检测和异常处理")
	fmt.Println("  - 负载均衡和流量控制")

	fmt.Println("\n✅ 高级追踪演示完成!")
}