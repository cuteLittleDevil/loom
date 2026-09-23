// Package loom 是固定容量的协程池。
// Submit 立刻返回容量为 1 的 channel。
// 协调者、执行者和告警分在 coordinator.go、executor.go、alert.go。
// 协调者协程独占运行表和计数，并按空闲槽位启动任务协程；用户函数只在这些协程上执行。
package loom

import (
	"fmt"
	"runtime/debug"
	"time"
)

// Config 是池子的创建参数。
type Config struct {
	// Size 是同时执行的用户函数上限，必须大于 0。
	Size int
	// OccupyThreshold 是单次任务的占用告警线。
	// 零值按 30s 处理；负值不启动巡检。
	OccupyThreshold time.Duration
	// OnAlert 在协调者之外调用，可以为 nil。
	// 为 nil 时仍然更新告警计数。不要在用户函数里同步接收本池的 channel。
	OnAlert func(Alert)
}

// Pool 是固定容量的协程池。
// 运行中的任务和全部计数只放在协调者的 sched 上。用户函数和 OnAlert 都在协调者之外执行。
type Pool struct {
	_ noCopy

	size      int
	threshold time.Duration
	onAlert   func(Alert)
	interval  time.Duration

	// operationFuncChan 缓冲 operationBuffer。
	// Submit 能放进缓冲就直接返回；放不下才另开协程阻塞发送。
	operationFuncChan chan operationFunc
}

// noCopy 让 go vet 拒绝复制 Pool。Lock 和 Unlock 是 vet 的标记，运行时不会加锁。
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// New 启动协调者。告警开启时再启动一个巡检 goroutine。
// 任务协程要等有空闲槽位才由协调者启动。Size <= 0 时返回 nil 和 ErrInvalidSize，不启动 goroutine。
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
		size:              cfg.Size,
		threshold:         threshold,
		onAlert:           cfg.OnAlert,
		operationFuncChan: make(chan operationFunc, operationBuffer),
	}
	if alerts {
		p.interval = min(threshold, time.Second)
	}
	go p.run()
	if alerts {
		go p.inspect()
	}
	return p, nil
}

// Submit 接受 fn 并立刻返回 channel。R 由 fn 推断。
// sign 是来源标记，原样记在运行中的任务和告警上，不参与调度。空字符串允许。
// channel 容量为 1，终态后送出一次 Result 并关闭。没人接收也不会堵住任务协程。
func (p *Pool) Submit[R any](sign string, fn func() (R, error), opts ...Option) <-chan Result[R] {
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
	job := &task{sign: sign, priority: priority}
	job.exec = func() {
		defer func() {
			if rec := recover(); rec != nil {
				job.err = &PanicError{Value: rec, Stack: debug.Stack()}
			}
		}()
		var err error
		value, err = fn()
		job.err = err
	}
	job.deliver = func(snap Snapshot) {
		deliverNow(ch, Result[R]{Value: value, Snapshot: snap, Err: job.err})
	}

	p.enqueue(func(st *sched) {
		st.accept(job, time.Now())
	})
	return ch
}

func deliverNow[R any](ch chan Result[R], res Result[R]) {
	ch <- res
	close(ch)
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
	Sign       string
	Priority   int
	WaitingFor time.Duration
	RunningFor time.Duration
	Alerted    bool
}

// Alert 是一次占用告警，描述决定发出它的那一刻。
type Alert struct {
	TaskID     uint64
	Sign       string
	Priority   int
	RunningFor time.Duration
	Idle       int
	Running    int
	Waiting    int
	At         time.Time
}

// PanicError 是用户函数 panic 被任务协程恢复后的错误。
// Unwrap 返回 ErrPanic。Stack 是任务协程在 recover 处的栈。
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("loom: task panic: %v", e.Value)
}

func (e *PanicError) Unwrap() error { return ErrPanic }
