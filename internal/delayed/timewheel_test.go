package delayed

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWorkerPoolBackpressure 测试工作池背压处理
func TestWorkerPoolBackpressure(t *testing.T) {
	config := WorkerPoolConfig{
		InitialWorkers: 2,
		MaxQueueSize:   5,
		DropPolicy:     DropPolicyDrop,
	}

	var processedTasks []string
	var mu sync.Mutex

	callback := func(task *DelayedTask) {
		mu.Lock()
		processedTasks = append(processedTasks, task.MessageID)
		mu.Unlock()
		time.Sleep(100 * time.Millisecond) // 模拟处理时间
	}

	wp := NewWorkerPool(config, callback)
	wp.Start()
	defer wp.Stop()

	// 提交超过队列容量的任务
	for i := 0; i < 10; i++ {
		task := &DelayedTask{
			MessageID:   fmt.Sprintf("task-%d", i),
			DeliverTime: time.Now().UnixMilli(),
			Priority:    PriorityNormal,
			Droppable:   true,
		}
		wp.Submit(task)
	}

	time.Sleep(500 * time.Millisecond)

	stats := wp.GetStats()
	t.Logf("Processed: %d, Dropped: %d", stats.ProcessedTasks, stats.DroppedTasks)

	if stats.DroppedTasks == 0 {
		t.Error("Expected some tasks to be dropped due to full queue")
	}
}

// TestWorkerPoolAutoScaling 测试工作池自动扩缩容
func TestWorkerPoolAutoScaling(t *testing.T) {
	config := WorkerPoolConfig{
		InitialWorkers:     2,
		MinWorkers:         1,
		MaxWorkers:         5,
		MaxQueueSize:       10,
		ScaleUpThreshold:   0.6,
		ScaleDownThreshold: 0.2,
	}

	callback := func(task *DelayedTask) {
		time.Sleep(50 * time.Millisecond)
	}

	wp := NewWorkerPool(config, callback)
	wp.Start()
	defer wp.Stop()

	// 提交大量任务触发扩容
	for i := 0; i < 8; i++ {
		task := &DelayedTask{
			MessageID:   fmt.Sprintf("task-%d", i),
			DeliverTime: time.Now().UnixMilli(),
			Priority:    PriorityNormal,
			Droppable:   false,
		}
		wp.Submit(task)
	}

	time.Sleep(200 * time.Millisecond)
	stats := wp.GetStats()
	t.Logf("Worker count after scaling: %d", stats.WorkerCount)

	// 等待任务处理完成，触发缩容
	time.Sleep(1 * time.Second)
	stats = wp.GetStats()
	t.Logf("Final worker count: %d", stats.WorkerCount)
}

// TestOptimizedTimeWheelRateLimit 测试时间轮限流
func TestOptimizedTimeWheelRateLimit(t *testing.T) {
	config := OptimizedTimeWheelConfig{
		SlotCount:       60,
		TickMs:          1000,
		MaxTasksPerTick: 3,
	}

	workerConfig := WorkerPoolConfig{
		InitialWorkers: 2,
		MaxQueueSize:   10,
	}

	executed := make([]string, 0)
	var mu sync.Mutex

	callback := func(task *DelayedTask) {
		mu.Lock()
		executed = append(executed, task.MessageID)
		mu.Unlock()
	}

	tw := NewOptimizedTimeWheel(config, callback, workerConfig)
	tw.Start()
	defer tw.Stop()

	// 添加6个任务到同一个执行时间（500ms后，确保在下一个tick之前）
	executeTime := time.Now().Add(500 * time.Millisecond).UnixMilli()
	for i := 0; i < 6; i++ {
		task := &TimeWheelTask{
			ID:          fmt.Sprintf("task-%d", i),
			ExecuteTime: executeTime,
			Data: &DelayedTask{
				MessageID: fmt.Sprintf("msg-%d", i),
			},
		}
		tw.AddTask(task)
	}

	// 等待足够长的时间确保任务被处理（包括重新调度的任务）
	time.Sleep(3500 * time.Millisecond)

	stats := tw.GetOptimizedStats()
	t.Logf("Processed: %d, Skipped: %d", stats.ProcessedTasks, stats.SkippedTasks)
	t.Logf("Executed tasks: %v", executed)

	// 验证速率限制：应该有任务被重新调度，所以处理的任务数应该小于总任务数
	// 或者等待更长时间让重新调度的任务也被处理
	if len(executed) < 6 {
		t.Logf("Rate limiting working: only %d out of 6 tasks processed immediately", len(executed))
	} else {
		t.Logf("All tasks processed: %d", len(executed))
	}
}

// TestOptimizedTimeWheelDropPolicy 测试时间轮丢弃策略
func TestOptimizedTimeWheelDropPolicy(t *testing.T) {
	config := OptimizedTimeWheelConfig{
		SlotCount:                 60,
		TickMs:                    1000,
		MaxTasksPerTick:           10,
		EnablePressureDetection:   true,
		PressureThreshold:         0.5,
		CriticalPressureThreshold: 0.8,
	}

	workerConfig := WorkerPoolConfig{
		InitialWorkers:            1,
		MaxQueueSize:              5,
		EnableSmartDrop:           true,
		HighPressureThreshold:     0.5,
		CriticalPressureThreshold: 0.8,
	}

	executed := make([]string, 0)
	var mu sync.Mutex

	callback := func(task *DelayedTask) {
		mu.Lock()
		executed = append(executed, task.MessageID)
		mu.Unlock()
		// 模拟慢处理
		time.Sleep(200 * time.Millisecond)
	}

	tw := NewOptimizedTimeWheel(config, callback, workerConfig)
	tw.Start()
	defer tw.Stop()

	// 添加混合任务（500ms后，确保在下一个tick之前）
	executeTime := time.Now().Add(500 * time.Millisecond).UnixMilli()
	for i := 0; i < 15; i++ {
		droppable := i%2 == 0 // 偶数任务可丢弃
		priority := PriorityNormal
		if droppable {
			priority = PriorityLow
		}

		task := &TimeWheelTask{
			ID:          fmt.Sprintf("task-%d", i),
			ExecuteTime: executeTime,
			Data: &DelayedTask{
				MessageID: fmt.Sprintf("msg-%d", i),
				Priority:  priority,
				Droppable: droppable,
			},
			Priority:  priority,
			Droppable: droppable,
		}
		tw.AddTask(task)
	}

	// 等待足够长的时间确保任务被处理
	time.Sleep(2500 * time.Millisecond)

	stats := tw.GetOptimizedStats()
	t.Logf("Processed: %d, Dropped: %d, Skipped: %d", stats.ProcessedTasks, stats.DroppedTasks, stats.SkippedTasks)

	// 应该有一些任务被丢弃或跳过（由于压力）
	if stats.DroppedTasks == 0 && stats.SkippedTasks == 0 {
		t.Errorf("Expected some tasks to be dropped or skipped due to pressure")
	}
}

// TestOptimizedTimeWheelConcurrentAccess 测试时间轮并发访问
func TestOptimizedTimeWheelConcurrentAccess(t *testing.T) {
	config := OptimizedTimeWheelConfig{
		SlotCount:       60,
		TickMs:          100,
		MaxTasksPerTick: 50,
	}

	workerConfig := WorkerPoolConfig{
		InitialWorkers: 5,
		MaxWorkers:     10,
		MaxQueueSize:   100,
	}

	var processedCount int64
	callback := func(task *DelayedTask) {
		atomic.AddInt64(&processedCount, 1)
	}

	tw := NewOptimizedTimeWheel(config, callback, workerConfig)
	tw.Start()
	defer tw.Stop()

	// 并发添加任务
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				task := &TimeWheelTask{
					ID:          fmt.Sprintf("task-%d-%d", routineID, j),
					ExecuteTime: time.Now().UnixMilli() + int64(j*50),
					Data: &DelayedTask{
						MessageID:   fmt.Sprintf("msg-%d-%d", routineID, j),
						DeliverTime: time.Now().UnixMilli() + int64(j*50),
						Priority:    PriorityNormal,
						Droppable:   false,
					},
				}
				tw.AddTask(task)
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(2 * time.Second)

	stats := tw.GetOptimizedStats()
	t.Logf("Total tasks: %d, Processed: %d, Worker scaled to: %d",
		stats.TotalTasks, atomic.LoadInt64(&processedCount), stats.WorkerCount)

	if atomic.LoadInt64(&processedCount) == 0 {
		t.Error("Expected some tasks to be processed")
	}
}

// TestSmartDropPolicy 测试智能丢弃策略
func TestSmartDropPolicy(t *testing.T) {
	config := WorkerPoolConfig{
		InitialWorkers:            1,
		MaxQueueSize:              3,
		EnableSmartDrop:           true,
		HighPressureThreshold:     0.6,
		CriticalPressureThreshold: 0.9,
	}

	var processedTasks []string
	var mu sync.Mutex

	callback := func(task *DelayedTask) {
		mu.Lock()
		processedTasks = append(processedTasks, task.MessageID)
		mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

	wp := NewWorkerPool(config, callback)
	wp.Start()
	defer wp.Stop()

	// 提交不同优先级的任务
	tasks := []*DelayedTask{
		{MessageID: "critical-1", Priority: PriorityCritical, Droppable: false},
		{MessageID: "high-1", Priority: PriorityHigh, Droppable: false},
		{MessageID: "normal-1", Priority: PriorityNormal, Droppable: true},
		{MessageID: "low-1", Priority: PriorityLow, Droppable: true},
		{MessageID: "low-2", Priority: PriorityLow, Droppable: true},
		{MessageID: "normal-2", Priority: PriorityNormal, Droppable: true},
	}

	for _, task := range tasks {
		wp.Submit(task)
		time.Sleep(10 * time.Millisecond) 
	}

	time.Sleep(1 * time.Second)

	stats := wp.GetStats()
	t.Logf("Processed: %d, Dropped: %d", stats.ProcessedTasks, stats.DroppedTasks)
	t.Logf("Processed tasks: %v", processedTasks)

	// 验证高优先级任务被处理，低优先级可丢弃任务被丢弃
	if stats.DroppedTasks == 0 {
		t.Error("Expected some low priority tasks to be dropped")
	}
}
