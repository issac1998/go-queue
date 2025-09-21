package delayed

import (
	"container/list"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// DelayedTask defines delayed task
type DelayedTask struct {
	MessageID   string
	DeliverTime int64
	Priority    MessagePriority
	Droppable   bool
}

// TimeWheelTask defines timewheel task
type TimeWheelTask struct {
	ID          string
	ExecuteTime int64
	Data        *DelayedTask
	Priority    MessagePriority
	Droppable   bool
	Callback    func(*DelayedTask)
}

// TimeWheelSlot 时间轮槽位
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

func (slot *TimeWheelSlot) GetTaskCount() int {
	slot.mutex.RLock()
	defer slot.mutex.RUnlock()
	return slot.tasks.Len()
}

// DropPolicy 丢弃策略
type DropPolicy int

const (
	DropPolicyBlock   DropPolicy = iota // 阻塞等待（默认）
	DropPolicyDrop                      // 丢弃新任务
	DropPolicyDropOld                   // 丢弃旧任务
)

// WorkerPoolConfig 工作池配置
type WorkerPoolConfig struct {
	InitialWorkers     int
	MinWorkers         int
	MaxWorkers         int
	MaxQueueSize       int
	DropPolicy         DropPolicy
	ScaleUpThreshold   float64 // 队列使用率超过此值时扩容
	ScaleDownThreshold float64 // 队列使用率低于此值时缩容

	// 智能丢弃配置
	EnableSmartDrop           bool    // 启用智能丢弃
	HighPressureThreshold     float64 // 高压力阈值(0.7)，开始丢弃低优先级任务
	CriticalPressureThreshold float64 // 临界压力阈值(0.9)，丢弃所有可丢弃任务
}

// WorkerPool 优化的工作池，支持背压处理和动态扩容
type WorkerPool struct {
	taskChan   chan *DelayedTask
	workerSize int32 // 使用原子操作
	maxWorkers int32
	minWorkers int32
	callback   func(*DelayedTask)
	stopChan   chan struct{}
	wg         sync.WaitGroup

	// 监控指标
	queueLength    int64 // 当前队列长度
	droppedTasks   int64 // 丢弃的任务数
	processedTasks int64 // 处理的任务数

	// 配置
	maxQueueSize       int
	dropPolicy         DropPolicy
	scaleUpThreshold   float64 // 扩容阈值
	scaleDownThreshold float64 // 缩容阈值

	// 智能丢弃配置
	enableSmartDrop           bool    // 启用智能丢弃
	highPressureThreshold     float64 // 高压力阈值，超过此值开始智能丢弃
	criticalPressureThreshold float64 // 临界压力阈值，超过此值丢弃所有可丢弃任务

	mutex sync.RWMutex
}

func NewWorkerPool(config WorkerPoolConfig, callback func(*DelayedTask)) *WorkerPool {
	wp := &WorkerPool{
		taskChan:                  make(chan *DelayedTask, config.MaxQueueSize),
		workerSize:                int32(config.InitialWorkers),
		maxWorkers:                int32(config.MaxWorkers),
		minWorkers:                int32(config.MinWorkers),
		callback:                  callback,
		stopChan:                  make(chan struct{}),
		maxQueueSize:              config.MaxQueueSize,
		dropPolicy:                config.DropPolicy,
		scaleUpThreshold:          config.ScaleUpThreshold,
		scaleDownThreshold:        config.ScaleDownThreshold,
		enableSmartDrop:           config.EnableSmartDrop,
		highPressureThreshold:     config.HighPressureThreshold,
		criticalPressureThreshold: config.CriticalPressureThreshold,
	}

	// 启动监控协程
	go wp.monitor()

	return wp
}

func (wp *WorkerPool) Start() {
	currentWorkers := atomic.LoadInt32(&wp.workerSize)
	for i := int32(0); i < currentWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.stopChan)
	wp.wg.Wait()
}

func (wp *WorkerPool) Submit(task *DelayedTask) bool {
	if wp.enableSmartDrop && wp.shouldDropTask(task) {
		atomic.AddInt64(&wp.droppedTasks, 1)
		log.Printf("Task dropped by smart drop policy: %s (priority: %d, droppable: %t)",
			task.MessageID, task.Priority, task.Droppable)
		return false
	}

	select {
	case wp.taskChan <- task:
		atomic.AddInt64(&wp.queueLength, 1)
		return true
	default:
		return wp.handleFullQueue(task)
	}
}

// shouldDropTask 判断是否应该丢弃任务
func (wp *WorkerPool) shouldDropTask(task *DelayedTask) bool {
	if !task.Droppable {
		return false
	}

	queueUsage := float64(len(wp.taskChan)) / float64(wp.maxQueueSize)

	// 高压力时丢弃低优先级任务
	if queueUsage >= wp.highPressureThreshold && task.Priority == PriorityLow {
		return true
	}

	// 临界压力时丢弃所有可丢弃任务
	if queueUsage >= wp.criticalPressureThreshold {
		return true
	}

	return false
}

func (wp *WorkerPool) handleFullQueue(task *DelayedTask) bool {
	switch wp.dropPolicy {
	case DropPolicyDrop:
		atomic.AddInt64(&wp.droppedTasks, 1)
		return false
	case DropPolicyDropOld:
		// 尝试丢弃一个旧任务
		select {
		case oldTask := <-wp.taskChan:
			atomic.AddInt64(&wp.droppedTasks, 1)
			log.Printf("Task dropped due to timeout: %s", oldTask.MessageID)
			// 添加新任务
			select {
			case wp.taskChan <- task:
				atomic.AddInt64(&wp.queueLength, 1)
				return true
			default:
				atomic.AddInt64(&wp.droppedTasks, 1)
				return false
			}
		default:
			atomic.AddInt64(&wp.droppedTasks, 1)
			return false
		}
	default: // DropPolicyBlock
		select {
		case wp.taskChan <- task:
			atomic.AddInt64(&wp.queueLength, 1)
			return true
		case <-wp.stopChan:
			return false
		}
	}
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	for {
		select {
		case task := <-wp.taskChan:
			atomic.AddInt64(&wp.queueLength, -1)
			atomic.AddInt64(&wp.processedTasks, 1)
			if wp.callback != nil {
				wp.callback(task)
			}
		case <-wp.stopChan:
			return
		}
	}
}

func (wp *WorkerPool) monitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wp.autoScale()
		case <-wp.stopChan:
			return
		}
	}
}

func (wp *WorkerPool) autoScale() {
	queueLength := atomic.LoadInt64(&wp.queueLength)
	currentWorkers := atomic.LoadInt32(&wp.workerSize)
	queueUsage := float64(queueLength) / float64(wp.maxQueueSize)

	// 扩容逻辑
	if queueUsage > wp.scaleUpThreshold && currentWorkers < wp.maxWorkers {
		newWorkers := currentWorkers + 1
		if atomic.CompareAndSwapInt32(&wp.workerSize, currentWorkers, newWorkers) {
			wp.wg.Add(1)
			go wp.worker()
		}
	}

	// 缩容逻辑
	if queueUsage < wp.scaleDownThreshold && currentWorkers > wp.minWorkers {
		newWorkers := currentWorkers - 1
		atomic.CompareAndSwapInt32(&wp.workerSize, currentWorkers, newWorkers)
		// 注意：这里不能直接停止worker，因为它们会在处理完当前任务后自然退出
	}
}

func (wp *WorkerPool) GetStats() WorkerPoolStats {
	return WorkerPoolStats{
		WorkerCount:    int(atomic.LoadInt32(&wp.workerSize)),
		QueueLength:    int(atomic.LoadInt64(&wp.queueLength)),
		DroppedTasks:   atomic.LoadInt64(&wp.droppedTasks),
		ProcessedTasks: atomic.LoadInt64(&wp.processedTasks),
		MaxQueueSize:   wp.maxQueueSize,
	}
}

type WorkerPoolStats struct {
	WorkerCount    int   `json:"worker_count"`
	QueueLength    int   `json:"queue_length"`
	DroppedTasks   int64 `json:"dropped_tasks"`
	ProcessedTasks int64 `json:"processed_tasks"`
	MaxQueueSize   int   `json:"max_queue_size"`
}

type OptimizedStats struct {
	WorkerCount    int32 `json:"worker_count"`
	QueueLength    int64 `json:"queue_length"`
	MaxQueueSize   int   `json:"max_queue_size"`
	ProcessedTasks int64 `json:"processed_tasks"`
	DroppedTasks   int64 `json:"dropped_tasks"`

	// 时间轮统计
	TickCount    int64 `json:"tick_count"`
	TotalTasks   int64 `json:"total_tasks"`
	SkippedTasks int64 `json:"skipped_tasks"`
}

// OptimizedTimeWheelConfig 优化时间轮配置
type OptimizedTimeWheelConfig struct {
	SlotCount       int
	TickMs          int64
	MaxTasksPerTick int // 每次tick最多处理的任务数

	// 压力检测配置
	EnablePressureDetection   bool    // 启用压力检测
	PressureThreshold         float64 // 压力阈值，超过此值开始丢弃可丢弃任务
	CriticalPressureThreshold float64 // 临界压力阈值，超过此值丢弃所有可丢弃任务
}

// OptimizedTimeWheel 优化的时间轮，支持限流和背压处理
type OptimizedTimeWheel struct {
	slots        []*TimeWheelSlot
	slotCount    int
	tickMs       int64
	currentSlot  int
	startTime    int64
	ticker       *time.Ticker
	stopChan     chan struct{}
	taskCallback func(*DelayedTask)
	mutex        sync.RWMutex
	running      bool
	workerPool   *WorkerPool

	// 限流配置
	maxTasksPerTick int // 每次tick最多处理的任务数

	// 压力检测配置
	enablePressureDetection   bool
	pressureThreshold         float64
	criticalPressureThreshold float64

	// 统计信息
	tickCount      int64
	processedTasks int64
	skippedTasks   int64
	droppedTasks   int64 // 时间轮层面丢弃的任务数
}

func NewOptimizedTimeWheel(config OptimizedTimeWheelConfig, callback func(*DelayedTask),
	workerConfig WorkerPoolConfig) *OptimizedTimeWheel {

	slots := make([]*TimeWheelSlot, config.SlotCount)
	for i := 0; i < config.SlotCount; i++ {
		slots[i] = NewTimeWheelSlot()
	}

	// 创建工作池
	workerPool := NewWorkerPool(workerConfig, callback)

	return &OptimizedTimeWheel{
		slots:                     slots,
		slotCount:                 config.SlotCount,
		tickMs:                    config.TickMs,
		currentSlot:               0,
		startTime:                 time.Now().UnixMilli(),
		stopChan:                  make(chan struct{}),
		taskCallback:              callback,
		running:                   false,
		workerPool:                workerPool,
		maxTasksPerTick:           config.MaxTasksPerTick,
		enablePressureDetection:   config.EnablePressureDetection,
		pressureThreshold:         config.PressureThreshold,
		criticalPressureThreshold: config.CriticalPressureThreshold,
	}
}

func (tw *OptimizedTimeWheel) SetMaxTasksPerTick(max int) {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()
	tw.maxTasksPerTick = max
}

func (tw *OptimizedTimeWheel) Start() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	if tw.running {
		return
	}

	tw.ticker = time.NewTicker(time.Duration(tw.tickMs) * time.Millisecond)
	tw.running = true

	// 启动工作池
	tw.workerPool.Start()

	go tw.run()
}

func (tw *OptimizedTimeWheel) Stop() {
	tw.mutex.Lock()
	defer tw.mutex.Unlock()

	if !tw.running {
		return
	}

	tw.running = false
	close(tw.stopChan)
	tw.ticker.Stop()

	// 停止工作池
	tw.workerPool.Stop()
}

func (tw *OptimizedTimeWheel) AddTask(task *TimeWheelTask) {
	tw.mutex.RLock()
	defer tw.mutex.RUnlock()

	if !tw.running {
		return
	}

	// 检查是否应该在添加时丢弃任务
	if tw.enablePressureDetection && tw.shouldDropTaskOnAdd(task.Data) {
		atomic.AddInt64(&tw.skippedTasks, 1)
		return
	}

	slotIndex := tw.calculateSlotIndex(task.ExecuteTime)
	tw.slots[slotIndex].AddTask(task)
}

// shouldDropTaskOnAdd 在添加任务时检查是否应该丢弃
func (tw *OptimizedTimeWheel) shouldDropTaskOnAdd(task *DelayedTask) bool {
	if !task.Droppable {
		return false
	}

	// 计算当前压力
	stats := tw.workerPool.GetStats()
	pressure := float64(stats.QueueLength) / float64(stats.MaxQueueSize)

	// 高压力时丢弃低优先级任务
	if pressure >= tw.pressureThreshold && task.Priority == PriorityLow {
		log.Printf("Task dropped by pressure filter: %s (priority: %d, pressure: %.2f)",
			task.MessageID, task.Priority, pressure)
		return true
	}

	return false
}

func (tw *OptimizedTimeWheel) calculateSlotIndex(executeTime int64) int {
	// 按秒为粒度计算槽位索引
	currentTime := time.Now().UnixMilli()
	
	delay := (executeTime - currentTime) / 1000 // 将毫秒差值转换为秒
	if delay < 0 {
		delay = 0
	}
	
	// 如果延迟超过60秒，则放到最后一个槽位
	if delay >= int64(tw.slotCount) {
		delay = int64(tw.slotCount) - 1
	}

	targetSlot := (tw.currentSlot + int(delay)) % tw.slotCount
	return targetSlot
}

func (tw *OptimizedTimeWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tick()
		case <-tw.stopChan:
			return
		}
	}
}

func (tw *OptimizedTimeWheel) tick() {
	atomic.AddInt64(&tw.tickCount, 1)
	currentTime := time.Now().UnixMilli()
	
	// 获取当前槽
	slot := tw.slots[tw.currentSlot]
	slot.mutex.Lock()
	defer slot.mutex.Unlock()

	var toRemove []*list.Element
	processedCount := 0
	skippedCount := 0

	// 计算当前压力
	var queuePressure float64
	if tw.enablePressureDetection && tw.workerPool != nil {
		stats := tw.workerPool.GetStats()
		queuePressure = float64(stats.QueueLength) / float64(stats.MaxQueueSize)
	}

	// 单轮遍历：直接处理过期任务
	for e := slot.tasks.Front(); e != nil; e = e.Next() {
		task := e.Value.(*TimeWheelTask)
		
		// 检查任务是否过期
		if task.ExecuteTime > currentTime {
			continue
		}
		
		// 标记要移除的任务
		toRemove = append(toRemove, e)
		
		// 检查是否达到每次tick的任务处理限制
		if tw.maxTasksPerTick > 0 && processedCount >= tw.maxTasksPerTick {
			// 重新调度超出限制的任务
			tw.rescheduleTask(task)
			skippedCount++
			continue
		}
		
		// 根据压力决定是否处理任务
		shouldProcess := true
		if tw.enablePressureDetection && task.Data != nil {
			if queuePressure >= tw.criticalPressureThreshold {
				// 临界压力：只处理不可丢弃的任务
				shouldProcess = !task.Data.Droppable
			} else if queuePressure >= tw.pressureThreshold {
				// 高压力：根据优先级决定
				shouldProcess = !task.Data.Droppable || task.Data.Priority >= PriorityHigh
			}
		}
		
		if shouldProcess && task.Data != nil {
			// 直接提交到工作池
			if tw.workerPool.Submit(task.Data) {
				processedCount++
			} else {
				skippedCount++
			}
		} else {
			// 任务被压力过滤丢弃
			skippedCount++
		}
	}

	// 移除已处理的任务
	for _, e := range toRemove {
		slot.tasks.Remove(e)
	}

	// 更新统计信息
	if processedCount > 0 {
		atomic.AddInt64(&tw.processedTasks, int64(processedCount))
	}
	if skippedCount > 0 {
		atomic.AddInt64(&tw.skippedTasks, int64(skippedCount))
	}

	tw.currentSlot = (tw.currentSlot + 1) % tw.slotCount
}

// filterTasksByPressure 根据压力过滤任务
func (tw *OptimizedTimeWheel) filterTasksByPressure(tasks []*TimeWheelTask) []*TimeWheelTask {
	if len(tasks) == 0 {
		return tasks
	}

	// 计算当前压力
	stats := tw.workerPool.GetStats()
	pressure := float64(stats.QueueLength) / float64(stats.MaxQueueSize)

	if pressure < tw.pressureThreshold {
		return tasks
	}

	var filteredTasks []*TimeWheelTask
	var droppedCount int

	for _, task := range tasks {
		shouldDrop := false

		if task.Data != nil && task.Data.Droppable {
			// 高压力时丢弃低优先级任务
			if pressure >= tw.pressureThreshold && task.Data.Priority == PriorityLow {
				shouldDrop = true
			}
			// 临界压力时丢弃所有可丢弃任务
			if pressure >= tw.criticalPressureThreshold {
				shouldDrop = true
			}
		}

		if shouldDrop {
			droppedCount++
			log.Printf("Task dropped by pressure filter: %s (priority: %d, pressure: %.2f)",
				task.Data.MessageID, task.Data.Priority, pressure)
		} else {
			filteredTasks = append(filteredTasks, task)
		}
	}

	if droppedCount > 0 {
		log.Printf("Dropped %d tasks due to high pressure (usage: %.2f)", droppedCount, pressure)
		atomic.AddInt64(&tw.droppedTasks, int64(droppedCount))
	}

	return filteredTasks
}

func (tw *OptimizedTimeWheel) rescheduleTask(task *TimeWheelTask) {
	// 将任务重新调度到下一个tick
	task.ExecuteTime += tw.tickMs
	slotIndex := tw.calculateSlotIndex(task.ExecuteTime)
	tw.slots[slotIndex].AddTask(task)
}

func (tw *OptimizedTimeWheel) GetOptimizedStats() OptimizedStats {
	workerStats := tw.workerPool.GetStats()
	return OptimizedStats{
		WorkerCount:    int32(workerStats.WorkerCount),
		QueueLength:    int64(workerStats.QueueLength),
		MaxQueueSize:   workerStats.MaxQueueSize,
		ProcessedTasks: workerStats.ProcessedTasks, // 使用工作池的统计
		DroppedTasks:   workerStats.DroppedTasks + atomic.LoadInt64(&tw.droppedTasks),
		TickCount:      atomic.LoadInt64(&tw.tickCount),
		TotalTasks:     workerStats.ProcessedTasks + atomic.LoadInt64(&tw.skippedTasks),
		SkippedTasks:   atomic.LoadInt64(&tw.skippedTasks),
	}
}

func (tw *OptimizedTimeWheel) GetOptimizedTimeWheelStats() OptimizedTimeWheelStats {
	tw.mutex.RLock()
	defer tw.mutex.RUnlock()

	stats := OptimizedTimeWheelStats{
		SlotCount:       tw.slotCount,
		TickMs:          tw.tickMs,
		CurrentSlot:     tw.currentSlot,
		Running:         tw.running,
		MaxTasksPerTick: tw.maxTasksPerTick,
		TickCount:       atomic.LoadInt64(&tw.tickCount),
		ProcessedTasks:  atomic.LoadInt64(&tw.processedTasks),
		SkippedTasks:    atomic.LoadInt64(&tw.skippedTasks),
		WorkerPoolStats: tw.workerPool.GetStats(),
	}

	// 计算每个槽位的任务数量
	stats.TaskCounts = make([]int, tw.slotCount)
	totalTasks := 0
	for i, slot := range tw.slots {
		count := slot.GetTaskCount()
		stats.TaskCounts[i] = count
		totalTasks += count
	}
	stats.TotalTasks = totalTasks

	return stats
}

type OptimizedTimeWheelStats struct {
	SlotCount       int             `json:"slot_count"`
	TickMs          int64           `json:"tick_ms"`
	CurrentSlot     int             `json:"current_slot"`
	Running         bool            `json:"running"`
	TotalTasks      int             `json:"total_tasks"`
	TaskCounts      []int           `json:"task_counts"`
	MaxTasksPerTick int             `json:"max_tasks_per_tick"`
	TickCount       int64           `json:"tick_count"`
	ProcessedTasks  int64           `json:"processed_tasks"`
	SkippedTasks    int64           `json:"skipped_tasks"`
	WorkerPoolStats WorkerPoolStats `json:"worker_pool_stats"`
}

// TimeWheel 为了向后兼容，提供 TimeWheel 类型别名
type TimeWheel = OptimizedTimeWheel

// NewTimeWheel 为了向后兼容，提供 NewTimeWheel 函数
func NewTimeWheel(slotCount int, tickMs int64, callback func(*DelayedTask)) *TimeWheel {
	config := OptimizedTimeWheelConfig{
		SlotCount:                 slotCount,
		TickMs:                    tickMs,
		MaxTasksPerTick:           100, // 默认值
		EnablePressureDetection:   false,
		PressureThreshold:         0.8,
		CriticalPressureThreshold: 0.95,
	}

	workerConfig := WorkerPoolConfig{
		InitialWorkers:            10,
		MinWorkers:                5,
		MaxWorkers:                50,
		MaxQueueSize:              1000,
		DropPolicy:                DropPolicyBlock,
		ScaleUpThreshold:          0.8,
		ScaleDownThreshold:        0.2,
		EnableSmartDrop:           false,
		HighPressureThreshold:     0.7,
		CriticalPressureThreshold: 0.9,
	}

	return NewOptimizedTimeWheel(config, callback, workerConfig)
}
