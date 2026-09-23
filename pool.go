// Package loom 是固定容量的协程池。
// Submit 立刻返回容量为 1 的 channel；用户函数只在 worker 上执行。
package loom

import (
	"container/heap"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

// Config 是池子的创建参数。
type Config struct {
	// Size 是 worker 数，必须大于 0。
	Size int
	// OccupyThreshold 是单次任务的占用告警线。
	// 零值按 30s 处理；负值不启动巡检。
	OccupyThreshold time.Duration
	// OnAlert 在锁外调用，可以为 nil。
	// 为 nil 时仍然更新告警计数。回调里不要同步等待 Close，也不要在用户函数里同步接收本池的 channel。
	OnAlert func(Alert)
}

// Pool 是固定数量 worker 的协程池。
// 调度状态由一把互斥锁保护。用户函数和 OnAlert 都在锁外执行。
type Pool struct {
	_ noCopy

	size      int
	threshold time.Duration
	onAlert   func(Alert)
	interval  time.Duration

	mu   sync.Mutex
	cond *sync.Cond

	closed    bool
	nextID    uint64
	idle      int
	runningN  int
	waitingN  int
	submitted uint64
	completed uint64
	failed    uint64
	alerted   uint64

	queue      taskQueue
	waitCount  map[int]int
	runningSet map[uint64]*task
	ready      []*task

	inspectWake chan struct{}
	wg          sync.WaitGroup
}

// noCopy 让 go vet 拒绝复制 Pool。
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// task 是一次已接受的提交。exec 与 deliver 由 Submit 按 R 闭包填充。
type task struct {
	id         uint64
	priority   int
	accepted   time.Time
	started    time.Time
	waitingFor time.Duration
	alerted    bool
	alertK     int64
	err        error

	exec    func()
	deliver func(Snapshot)
}

// New 启动 Size 个 worker。告警开启时再启动一个巡检 goroutine。
// Size <= 0 时返回 nil 和 ErrInvalidSize，不启动 goroutine。
func New(cfg Config) (*Pool, error) {
	if cfg.Size <= 0 {
		return nil, ErrInvalidSize
	}
	threshold := cfg.OccupyThreshold
	alerts := threshold >= 0
	if threshold == 0 {
		threshold = 30 * time.Second
	}
	p := &Pool{
		size:       cfg.Size,
		threshold:  threshold,
		onAlert:    cfg.OnAlert,
		idle:       cfg.Size,
		nextID:     1,
		waitCount:  make(map[int]int),
		runningSet: make(map[uint64]*task),
	}
	p.cond = sync.NewCond(&p.mu)
	if alerts {
		p.interval = min(threshold, time.Second)
		p.inspectWake = make(chan struct{}, 1)
	}

	p.wg.Add(cfg.Size)
	for range cfg.Size {
		go p.worker()
	}
	if alerts {
		p.wg.Add(1)
		go p.inspect()
	}
	return p, nil
}

// Close 拒绝后续提交，并等待已接受的任务终态、结果送出、worker 和巡检退出。
// 同一个池上多次或并发调用，等的是同一次排空，结束后返回 nil。
// p == nil 时返回 ErrNilPool。
func (p *Pool) Close() error {
	if p == nil {
		return ErrNilPool
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.cond.Broadcast()
		p.wakeInspector()
	}
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

// Submit 接受 fn 并立刻返回 channel。R 由 fn 推断。
// channel 容量为 1，终态后送出一次 Result 并关闭。没人接收也不会堵住 worker。
func (p *Pool) Submit[R any](fn func() (R, error), opts ...Option) <-chan Result[R] {
	ch := make(chan Result[R], 1)
	if p == nil {
		deliverNow(ch, Result[R]{Err: ErrNilPool})
		return ch
	}
	if fn == nil {
		deliverNow(ch, Result[R]{Err: ErrNilFunc})
		return ch
	}

	priority := 0
	for _, opt := range opts {
		if opt != nil {
			priority = opt(priority)
		}
	}

	var value R
	t := &task{priority: priority}
	t.exec = func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.err = &PanicError{Value: rec, Stack: debug.Stack()}
			}
		}()
		var err error
		value, err = fn()
		t.err = err
	}
	t.deliver = func(snap Snapshot) {
		deliverNow(ch, Result[R]{Value: value, Snapshot: snap, Err: t.err})
	}

	if snap, rejected := p.accept(t); rejected {
		deliverNow(ch, Result[R]{Snapshot: snap, Err: ErrClosed})
	}
	return ch
}

func deliverNow[R any](ch chan Result[R], res Result[R]) {
	ch <- res
	close(ch)
}

// accept 在池锁里接受任务，或在已关闭时返回实况快照。
func (p *Pool) accept(t *task) (Snapshot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return p.snapshot(time.Now()), true
	}
	t.accepted = time.Now()
	t.id = p.nextID
	p.nextID++
	p.submitted++
	if p.idle > 0 {
		p.idle--
		p.markRunning(t, time.Now())
		p.ready = append(p.ready, t)
		p.cond.Signal()
		return Snapshot{}, false
	}
	heap.Push(&p.queue, t)
	p.waitingN++
	p.waitCount[t.priority]++
	return Snapshot{}, false
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		t := p.next()
		if t == nil {
			return
		}
		t.exec()
		snap := p.finish(t)
		t.deliver(snap)
	}
}

// next 取走已经标成 Running 并交给 worker 的任务。
// 已关闭且没有在途任务时，当前 worker 退出，不再计入 Idle。
func (p *Pool) next() *task {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.ready) == 0 {
		if p.closed && p.runningN == 0 && p.waitingN == 0 {
			p.idle--
			p.cond.Broadcast()
			return nil
		}
		p.cond.Wait()
	}
	t := p.ready[0]
	p.ready[0] = nil
	p.ready = p.ready[1:]
	return t
}

// finish 在同一临界区里计入终态、分派下一个排队任务，并复制快照。
// 队列非空时这个 worker 不会被算成空闲。
func (p *Pool) finish(t *task) Snapshot {
	p.mu.Lock()
	now := time.Now()
	delete(p.runningSet, t.id)
	p.runningN--
	p.completed++
	if t.err != nil {
		p.failed++
	}
	if p.queue.Len() > 0 {
		next := heap.Pop(&p.queue).(*task)
		p.waitingN--
		p.waitCount[next.priority]--
		if p.waitCount[next.priority] == 0 {
			delete(p.waitCount, next.priority)
		}
		p.markRunning(next, now)
		p.ready = append(p.ready, next)
		p.cond.Signal()
	} else {
		p.idle++
	}
	if p.closed && p.runningN == 0 && p.waitingN == 0 {
		p.wakeInspector()
	}
	snap := p.snapshot(now)
	p.mu.Unlock()
	return snap
}

func (p *Pool) markRunning(t *task, now time.Time) {
	t.started = now
	t.waitingFor = now.Sub(t.accepted)
	p.runningN++
	p.runningSet[t.id] = t
}

func (p *Pool) snapshot(now time.Time) Snapshot {
	snap := Snapshot{
		Size:      p.size,
		Idle:      p.idle,
		Running:   p.runningN,
		Waiting:   p.waitingN,
		Submitted: p.submitted,
		Completed: p.completed,
		Failed:    p.failed,
		Alerted:   p.alerted,
	}
	if n := len(p.runningSet); n > 0 {
		tasks := make([]*task, 0, n)
		for _, t := range p.runningSet {
			tasks = append(tasks, t)
		}
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].id < tasks[j].id })
		snap.RunningTasks = make([]TaskInfo, len(tasks))
		for i, t := range tasks {
			snap.RunningTasks[i] = TaskInfo{
				ID:         t.id,
				Priority:   t.priority,
				WaitingFor: t.waitingFor,
				RunningFor: now.Sub(t.started),
				Alerted:    t.alerted,
			}
		}
	}
	if n := len(p.waitCount); n > 0 {
		snap.WaitingBy = make([]PriorityCount, 0, n)
		for priority, count := range p.waitCount {
			if count > 0 {
				snap.WaitingBy = append(snap.WaitingBy, PriorityCount{Priority: priority, Count: count})
			}
		}
		sort.Slice(snap.WaitingBy, func(i, j int) bool {
			return snap.WaitingBy[i].Priority > snap.WaitingBy[j].Priority
		})
	}
	return snap
}

func (p *Pool) inspect() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.emitAlerts()
		case <-p.inspectWake:
		}
		p.mu.Lock()
		done := p.closed && p.runningN == 0 && p.waitingN == 0
		p.mu.Unlock()
		if done {
			return
		}
	}
}

// emitAlerts 在锁内合并跨过的周期，解锁后再按任务 ID 顺序调用 OnAlert。
func (p *Pool) emitAlerts() {
	p.mu.Lock()
	alerts := p.collectAlerts(time.Now())
	p.mu.Unlock()
	if p.onAlert == nil {
		return
	}
	for _, alert := range alerts {
		func() {
			defer func() { recover() }()
			p.onAlert(alert)
		}()
	}
}

func (p *Pool) collectAlerts(now time.Time) []Alert {
	if p.threshold <= 0 || len(p.runningSet) == 0 {
		return nil
	}
	tasks := make([]*task, 0, len(p.runningSet))
	for _, t := range p.runningSet {
		tasks = append(tasks, t)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].id < tasks[j].id })

	var alerts []Alert
	for _, t := range tasks {
		elapsed := now.Sub(t.started)
		periods := int64(elapsed / p.threshold)
		if periods <= t.alertK {
			continue
		}
		t.alertK = periods
		t.alerted = true
		p.alerted++
		alerts = append(alerts, Alert{
			TaskID:     t.id,
			Priority:   t.priority,
			RunningFor: elapsed,
			Idle:       p.idle,
			Running:    p.runningN,
			Waiting:    p.waitingN,
			At:         now,
		})
	}
	return alerts
}

func (p *Pool) wakeInspector() {
	if p.inspectWake == nil {
		return
	}
	select {
	case p.inspectWake <- struct{}{}:
	default:
	}
}

// taskQueue 是最大优先级堆。优先级数值更大的先出；相同则 ID 更小的先出。
type taskQueue []*task

func (q taskQueue) Len() int { return len(q) }

func (q taskQueue) Less(i, j int) bool {
	if q[i].priority != q[j].priority {
		return q[i].priority > q[j].priority
	}
	return q[i].id < q[j].id
}

func (q taskQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *taskQueue) Push(x any) { *q = append(*q, x.(*task)) }

func (q *taskQueue) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return item
}

// Option 调整一次提交。nil 项被忽略。
type Option func(priority int) int

// WithPriority 设置优先级。数值更大的先执行。多个同时出现时，最后一个生效。
func WithPriority(priority int) Option {
	return func(int) int { return priority }
}

// Result 是一次提交的终态。快照属于终态那一刻，不是接收 channel 的那一刻。
type Result[R any] struct {
	Value    R
	Snapshot Snapshot
	Err      error
}

// Snapshot 是池内调度状态的一份副本。
type Snapshot struct {
	Size         int
	Idle         int
	Running      int
	Waiting      int
	WaitingBy    []PriorityCount
	RunningTasks []TaskInfo
	Submitted    uint64
	Completed    uint64
	Failed       uint64
	Alerted      uint64
}

// PriorityCount 是某个优先级上的排队数量。
type PriorityCount struct {
	Priority int
	Count    int
}

// TaskInfo 描述一个已标成 Running、尚未终态的任务。
type TaskInfo struct {
	ID         uint64
	Priority   int
	WaitingFor time.Duration
	RunningFor time.Duration
	Alerted    bool
}

// Alert 是一次占用告警，描述决定发出它的那一刻。
type Alert struct {
	TaskID     uint64
	Priority   int
	RunningFor time.Duration
	Idle       int
	Running    int
	Waiting    int
	At         time.Time
}

// PanicError 是用户函数 panic 被 worker 恢复后的错误。
// Unwrap 返回 ErrPanic。Stack 是 worker goroutine 在 recover 处的栈。
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("loom: task panic: %v", e.Value)
}

func (e *PanicError) Unwrap() error { return ErrPanic }
