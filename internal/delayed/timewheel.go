package delayed

import (
	"container/list"
	"sync"
	"time"
)

// TimeWheelTask 时间轮任务
type TimeWheelTask struct {
	ID          string
	ExecuteTime int64 // 执行时间戳(毫秒)
	Data        interface{}
	Callback    func(interface{})
}

// TimeWheelSlot 时间轮槽位
type TimeWheelSlot struct {
	tasks *list.List
	mutex sync.RWMutex
}

// NewTimeWheelSlot 创建新的时间轮槽位
func NewTimeWheelSlot() *TimeWheelSlot {
	return &TimeWheelSlot{
		tasks: list.New(),
	}
}

// AddTask 添加任务到槽位
func (slot *TimeWheelSlot) AddTask(task *TimeWheelTask) {
	slot.mutex.Lock()
	defer slot.mutex.Unlock()
	slot.tasks.PushBack(task)
}

// GetExpiredTasks 获取已到期的任务
func (slot *TimeWheelSlot) GetExpiredTasks(currentTime int64) []*TimeWheelTask {
	slot.mutex.Lock()
	defer slot.mutex.Unlock()

	var expiredTasks []*TimeWheelTask
	var toRemove []*list.Element

	for e := slot.tasks.Front(); e != nil; e = e.Next() {
		task := e.Value.(*TimeWheelTask)
		if task.ExecuteTime <= currentTime {
			expiredTasks = append(expiredTasks, task)
			toRemove = append(toRemove, e)
		}
	}

	// 移除已到期的任务
	for _, e := range toRemove {
		slot.tasks.Remove(e)
	}

	return expiredTasks
}

// TimeWheel 时间轮调度器
type TimeWheel struct {
	slots        []*TimeWheelSlot
	slotCount    int
	tickMs       int64 // 刻度时间(毫秒)
	currentSlot  int
	startTime    int64
	ticker       *time.Ticker
	stopChan     chan struct{}
	taskCallback func(*TimeWheelTask)
	mutex        sync.RWMutex
	running      bool
}

// NewTimeWheel 创建新的时间轮
func NewTimeWheel(slotCount int, tickMs int64, callback func(*TimeWheelTask)) *TimeWheel {
	slots := make([]*TimeWheelSlot, slotCount)
	for i := 0; i < slotCount; i++ {
		slots[i] = NewTimeWheelSlot()
	}

	return &TimeWheel{
		slots:        slots,
		slotCount:    slotCount,
		tickMs:       tickMs,
		currentSlot:  0,
		startTime:    time.Now().UnixMilli(),
		stopChan:     make(chan struct{}),
		taskCallback: callback,
		running:      false,
	}
}

// Start 启动时间轮
func (tw *TimeWheel) Start() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	if tw.running {
		return
	}

	tw.ticker = time.NewTicker(time.Duration(tw.tickMs) * time.Millisecond)
	tw.running = true

	go tw.run()
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	if !tw.running {
		return
	}

	tw.running = false
	close(tw.stopChan)
	tw.ticker.Stop()
}

// AddTask 添加任务到时间轮
func (tw *TimeWheel) AddTask(task *TimeWheelTask) {
	tw.mutex.RLock()
	defer tw.mutex.RUnlock()

	if !tw.running {
		return
	}

	// 计算任务应该放在哪个槽位
	slotIndex := tw.calculateSlotIndex(task.ExecuteTime)
	tw.slots[slotIndex].AddTask(task)
}

// calculateSlotIndex 计算任务应该放在哪个槽位
func (tw *TimeWheel) calculateSlotIndex(executeTime int64) int {
	// 计算相对于当前时间的延迟
	delay := executeTime - time.Now().UnixMilli()
	if delay < 0 {
		delay = 0
	}

	// 计算需要多少个tick
	ticks := delay / tw.tickMs

	// 计算目标槽位
	targetSlot := (tw.currentSlot + int(ticks)) % tw.slotCount
	return targetSlot
}

// run 时间轮运行循环
func (tw *TimeWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tick()
		case <-tw.stopChan:
			return
		}
	}
}

// tick 时间轮滴答
func (tw *TimeWheel) tick() {
	currentTime := time.Now().UnixMilli()

	// 获取当前槽位的已到期任务
	expiredTasks := tw.slots[tw.currentSlot].GetExpiredTasks(currentTime)

	// 执行已到期的任务
	for _, task := range expiredTasks {
		if tw.taskCallback != nil {
			// 异步执行任务，避免阻塞时间轮
			go tw.taskCallback(task)
		}
	}

	// 移动到下一个槽位
	tw.currentSlot = (tw.currentSlot + 1) % tw.slotCount
}

// GetStats 获取时间轮统计信息
func (tw *TimeWheel) GetStats() TimeWheelStats {
	tw.mutex.RLock()
	defer tw.mutex.RUnlock()

	stats := TimeWheelStats{
		SlotCount:   tw.slotCount,
		TickMs:      tw.tickMs,
		CurrentSlot: tw.currentSlot,
		Running:     tw.running,
		TaskCounts:  make([]int, tw.slotCount),
	}

	totalTasks := 0
	for i, slot := range tw.slots {
		slot.mutex.RLock()
		count := slot.tasks.Len()
		slot.mutex.RUnlock()

		stats.TaskCounts[i] = count
		totalTasks += count
	}

	stats.TotalTasks = totalTasks
	return stats
}

// TimeWheelStats 时间轮统计信息
type TimeWheelStats struct {
	SlotCount   int   `json:"slot_count"`
	TickMs      int64 `json:"tick_ms"`
	CurrentSlot int   `json:"current_slot"`
	Running     bool  `json:"running"`
	TotalTasks  int   `json:"total_tasks"`
	TaskCounts  []int `json:"task_counts"`
}
