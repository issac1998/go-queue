package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/issac1998/go-queue/internal/async"
)

// CallbackTracker 回调追踪器
type CallbackTracker struct {
	mu       sync.RWMutex
	callbacks map[uint64]*CallbackInfo
}

type CallbackInfo struct {
	RequestID string
	Operation string
	StartTime time.Time
	UserData  uint64
}

func NewCallbackTracker() *CallbackTracker {
	return &CallbackTracker{
		callbacks: make(map[uint64]*CallbackInfo),
	}
}

func (ct *CallbackTracker) StartCallback(userData uint64, requestID, operation string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	
	ct.callbacks[userData] = &CallbackInfo{
		RequestID: requestID,
		Operation: operation,
		StartTime: time.Now(),
		UserData:  userData,
	}
	
	log.Printf("🚀 [CALLBACK-START] userData=0x%x, requestID=%s, operation=%s", 
		userData, requestID, operation)
}

func (ct *CallbackTracker) CompleteCallback(userData uint64, success bool, result string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	
	info, exists := ct.callbacks[userData]
	if !exists {
		log.Printf("⚠️  [CALLBACK-WARN] userData=0x%x not found", userData)
		return
	}
	
	duration := time.Since(info.StartTime)
	
	if success {
		log.Printf("✅ [CALLBACK-SUCCESS] userData=0x%x, requestID=%s, duration=%v, result=%s", 
			userData, info.RequestID, duration, result)
	} else {
		log.Printf("❌ [CALLBACK-FAILED] userData=0x%x, requestID=%s, duration=%v, error=%s", 
			userData, info.RequestID, duration, result)
	}
	
	delete(ct.callbacks, userData)
}

// DemoCallbackUsage 演示回调中UserData的使用
func DemoCallbackUsage() {
	fmt.Println("\n🔥 UserData 在回调中的实际使用演示")
	fmt.Println("====================================")
	
	tracker := NewCallbackTracker()
	
	// 创建异步IO配置
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
	
	fmt.Println("\n📡 模拟异步回调操作...")
	
	// 模拟多个异步操作
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// 生成唯一的userData
			connID := int64(id + 1)
			timestamp := time.Now().UnixNano() & 0xFFFFFFFF
			userData := uint64(connID)<<32 | uint64(timestamp)
			
			requestID := fmt.Sprintf("REQ-%d", id)
			operation := fmt.Sprintf("async-operation-%d", id)
			
			// 开始追踪
			tracker.StartCallback(userData, requestID, operation)
			
			// 模拟异步操作延迟
			time.Sleep(time.Duration(50+id*30) * time.Millisecond)
			
			// 模拟回调函数被调用
			// 🔥 这里就是真正使用 userData 的地方！
			if id%4 == 0 {
				// 模拟失败回调
				tracker.CompleteCallback(userData, false, fmt.Sprintf("模拟错误-%d", id))
			} else {
				// 模拟成功回调
				tracker.CompleteCallback(userData, true, fmt.Sprintf("成功结果-%d", id))
			}
		}(i)
	}
	
	wg.Wait()
	
	fmt.Println("\n🎯 关键理解:")
	fmt.Println("  1. userData 是异步操作的唯一标识符")
	fmt.Println("  2. 在 WriteAsync/ReadAsync 回调中，userData 让我们知道这是哪个请求")
	fmt.Println("  3. 通过 userData，我们可以:")
	fmt.Println("     - 关联请求和响应")
	fmt.Println("     - 追踪操作的完整生命周期")
	fmt.Println("     - 在错误时定位具体的请求")
	fmt.Println("     - 进行性能分析和调试")
	
	fmt.Println("\n💡 在实际的 Producer.sendWithAsyncConnection 中:")
	fmt.Println("  - WriteAsync 回调接收 userData，知道是哪个发送操作")
	fmt.Println("  - ReadAsync 回调接收同样的 userData，知道是哪个读取操作")
	fmt.Println("  - 错误处理时，userData 帮助我们定位具体的失败请求")
	fmt.Println("  - 成功时，userData 帮助我们关联请求和结果")
	
	fmt.Println("\n🔥 这就是为什么 userData 如此重要!")
	fmt.Println("   它不是装饰品，而是异步编程中请求追踪的核心机制!")
}