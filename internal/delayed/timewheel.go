package delayed

import (
	"container/list"
	"sync"
	"time"
)

// TimeWheelTask time wheel task
type TimeWheelTask struct {
	ID          string
	ExecuteTime int64
	Data        interface{}
	Callback    func(interface{})
}

// TimeWheelSlot time wheel slot
type TimeWheelSlot struct {
	tasks *list.List
	mutex sync.RWMutex
}

func NewTimeWheelSlot() *TimeWheelSlot {
	return &TimeWheelSlot{
		tasks: list.New(),
	}
}

func (slot *TimeWheelSlot) AddTask(task *TimeWheelTask) {
	slot.mutex.Lock()
	defer slot.mutex.Unlock()
	slot.tasks.PushBack(task)
}

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

	for _, e := range toRemove {
		slot.tasks.Remove(e)
	}

	return expiredTasks
}

// TimeWheel time wheel scheduler
type TimeWheel struct {
	slots        []*TimeWheelSlot
	slotCount    int
	tickMs       int64
	currentSlot  int
	startTime    int64
	ticker       *time.Ticker
	stopChan     chan struct{}
	taskCallback func(*TimeWheelTask)
	mutex        sync.RWMutex
	running      bool
}

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

// Start starts the time wheel
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

func (tw *TimeWheel) AddTask(task *TimeWheelTask) {
	tw.mutex.RLock()
	defer tw.mutex.RUnlock()

	if !tw.running {
		return
	}

	slotIndex := tw.calculateSlotIndex(task.ExecuteTime)
	tw.slots[slotIndex].AddTask(task)
}

func (tw *TimeWheel) calculateSlotIndex(executeTime int64) int {
	delay := executeTime - time.Now().UnixMilli()
	if delay < 0 {
		delay = 0
	}

	ticks := delay / tw.tickMs
	targetSlot := (tw.currentSlot + int(ticks)) % tw.slotCount
	return targetSlot
}

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

func (tw *TimeWheel) tick() {
	currentTime := time.Now().UnixMilli()
	expiredTasks := tw.slots[tw.currentSlot].GetExpiredTasks(currentTime)

	for _, task := range expiredTasks {
		if tw.taskCallback != nil {
			go tw.taskCallback(task)
		}
	}

	tw.currentSlot = (tw.currentSlot + 1) % tw.slotCount
}

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

// TimeWheelStats time wheel statistics
type TimeWheelStats struct {
	SlotCount   int   `json:"slot_count"`
	TickMs      int64 `json:"tick_ms"`
	CurrentSlot int   `json:"current_slot"`
	Running     bool  `json:"running"`
	TotalTasks  int   `json:"total_tasks"`
	TaskCounts  []int `json:"task_counts"`
}
