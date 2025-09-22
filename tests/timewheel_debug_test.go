package tests

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/issac1998/go-queue/internal/delayed"
)

// TestTimeWheelDebug 调试时间轮问题
func TestTimeWheelDebug(t *testing.T) {
	log.Println("=== 开始时间轮调试测试 ===")

	var mutex sync.Mutex
	executedTasks := make([]string, 0)

	callback := func(task *delayed.TimeWheelTask) {
		mutex.Lock()
		defer mutex.Unlock()
		executedTasks = append(executedTasks, task.ID)
		log.Printf("执行任务: %s, 执行时间: %s", task.ID, time.Unix(task.ExecuteTime/1000, 0).Format("15:04:05.000"))
	}

	// 创建时间轮 (10个槽位，1000ms精度)
	timeWheel := delayed.NewTimeWheel(10, 1000, callback)
	timeWheel.Start()
	defer timeWheel.Stop()

	startTime := time.Now()
	log.Printf("开始时间: %s", startTime.Format("15:04:05.000"))

	// 添加3个任务，都在1秒后执行
	for i := 0; i < 3; i++ {
		task := &delayed.TimeWheelTask{
			ID:          fmt.Sprintf("task-%d", i),
			ExecuteTime: startTime.Add(1*time.Second + time.Duration(i*100)*time.Millisecond).UnixMilli(),
			Data:        fmt.Sprintf("data-%d", i),
		}
		timeWheel.AddTask(task)
		log.Printf("添加任务: %s, 执行时间: %s", task.ID, time.Unix(task.ExecuteTime/1000, 0).Format("15:04:05.000"))
	}

	// 等待所有任务执行
	time.Sleep(3 * time.Second)

	mutex.Lock()
	defer mutex.Unlock()

	log.Printf("执行的任务数: %d", len(executedTasks))
	for i, taskID := range executedTasks {
		log.Printf("任务 %d: %s", i, taskID)
	}

	// 验证所有任务都被执行
	if len(executedTasks) != 3 {
		t.Fatalf("执行任务数量不正确: 期望3个, 实际%d个", len(executedTasks))
	}

	// 验证所有任务都被执行（顺序可能不同，因为是异步执行）
	expectedTasks := map[string]bool{"task-0": false, "task-1": false, "task-2": false}
	for _, taskID := range executedTasks {
		if _, exists := expectedTasks[taskID]; exists {
			expectedTasks[taskID] = true
		}
	}

	for taskID, executed := range expectedTasks {
		if !executed {
			t.Fatalf("任务 %s 未被执行", taskID)
		}
	}

	log.Println("=== 时间轮调试测试完成 ===")
}
