package loom

import (
	"container/heap"
	"sort"
	"time"
)

// operationBuffer 是交给协调者的操作缓冲。未满时 Submit 不另开协程。
const operationBuffer = 100

// operationFunc 只在协调者协程里执行。协调者之外不读写 sched。
type operationFunc func(st *sched)

// sched 只存在于协调者协程。running 是唯一的任务表。
type sched struct {
	size      int
	threshold time.Duration

	nextID uint64
	// submitted、completed、failed、alerted 只在这一个协调者里累加。
	// Idle、Running、Waiting 不另记，快照时从 running 和 queue 算。
	submitted uint64
	completed uint64
	failed    uint64
	alerted   uint64

	queue     taskQueue
	waitCount map[int]int
	running   map[uint64]*task

	// ready 里的任务已经标成 Running，但要等当前任务把 Result 送出后才启动。
	// 多个任务可以先后 finish、尚未 release，所以这里要留住一串。
	ready []*task
	// launch 是本次操作结束后要启动的那一个任务。每次操作最多产生一个。
	launch *task
}

func newSched(p *Pool) *sched {
	return &sched{
		size:      p.size,
		threshold: p.threshold,
		nextID:    1,
		waitCount: make(map[int]int),
		running:   make(map[uint64]*task),
	}
}

// run 是协调者。running 和全部计数只在这里读写。
func (p *Pool) run() {
	st := newSched(p)
	for op := range p.operationFuncChan {
		op(st)
		p.startTask(st)
	}
}

// enqueue 把操作放进协调者的缓冲。缓冲满时另开协程，由那条协程阻塞发送。
// 调用方不等待协调者执行完。
func (p *Pool) enqueue(fn operationFunc) {
	select {
	case p.operationFuncChan <- fn:
	default:
		go func() {
			p.operationFuncChan <- fn
		}()
	}
}

// operate 把操作交给协调者并等它执行完。
// finish、release 和告警采集要用结果，所以仍然等待。
func (p *Pool) operate(fn operationFunc) {
	done := make(chan struct{})
	p.operationFuncChan <- func(st *sched) {
		defer close(done)
		fn(st)
	}
	<-done
}

// startTask 在协调者上启动这一个任务协程。调用时协调者不在接收操作，
// 新协程若立刻回来发送，会堵住直到本轮操作结束，不会和当前操作重入。
func (p *Pool) startTask(st *sched) {
	job := st.launch
	if job == nil {
		return
	}
	st.launch = nil
	go p.execute(job)
}

// accept 在协调者里接受任务。
func (st *sched) accept(job *task, now time.Time) {
	job.accepted = now
	job.id = st.nextID
	st.nextID++
	st.submitted++
	if len(st.running) < st.size {
		st.markRunning(job, now)
		st.launch = job
		return
	}
	heap.Push(&st.queue, job)
	st.waitCount[job.priority]++
}

// finish 在协调者里计入终态、把下一个排队任务标成 Running，并复制快照。
// 队列非空时运行表长度不变，快照里的 Idle 不会变多。任务协程要等 release 才真正启动下一个用户函数。
func (st *sched) finish(job *task, now time.Time) Snapshot {
	delete(st.running, job.id)
	st.completed++
	if job.err != nil {
		st.failed++
	}
	if st.queue.Len() > 0 {
		next := heap.Pop(&st.queue).(*task)
		st.waitCount[next.priority]--
		if st.waitCount[next.priority] == 0 {
			delete(st.waitCount, next.priority)
		}
		st.markRunning(next, now)
		st.ready = append(st.ready, next)
	}
	return st.snapshot(now)
}

// release 在 Result 已经送出后调用。这时才让协调者启动下一个任务协程。
func (st *sched) release() {
	if len(st.ready) == 0 {
		return
	}
	next := st.ready[0]
	st.ready[0] = nil
	st.ready = st.ready[1:]
	st.launch = next
}

func (st *sched) markRunning(job *task, now time.Time) {
	job.started = now
	job.waitingFor = now.Sub(job.accepted)
	st.running[job.id] = job
}

func (st *sched) counts() (idle, running, waiting int) {
	running = len(st.running)
	waiting = st.queue.Len()
	idle = st.size - running
	return
}

func (st *sched) snapshot(now time.Time) Snapshot {
	idle, running, waiting := st.counts()
	snap := Snapshot{
		Size:      st.size,
		Idle:      idle,
		Running:   running,
		Waiting:   waiting,
		Submitted: st.submitted,
		Completed: st.completed,
		Failed:    st.failed,
		Alerted:   st.alerted,
	}
	if n := len(st.running); n > 0 {
		jobs := make([]*task, 0, n)
		for _, job := range st.running {
			jobs = append(jobs, job)
		}
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })
		snap.RunningTasks = make([]TaskInfo, len(jobs))
		for i, job := range jobs {
			snap.RunningTasks[i] = TaskInfo{
				ID:         job.id,
				Priority:   job.priority,
				WaitingFor: job.waitingFor,
				RunningFor: now.Sub(job.started),
				Alerted:    job.alerted,
			}
		}
	}
	if n := len(st.waitCount); n > 0 {
		snap.WaitingBy = make([]PriorityCount, 0, n)
		for priority, count := range st.waitCount {
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
