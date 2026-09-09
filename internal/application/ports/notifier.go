package ports

// Notifier 唤醒通知端口（详设 §3.3）：非阻塞，实现为池向所有 worker 的 cap-1
// channel 投递；已满则合并本次唤醒，任务事实始终在数据库，丢失由轮询兜底。
type Notifier interface {
	Notify()
}
