package loom

import (
	"container/heap"
	"sort"
	"time"
)

// operationFunc 只在协调者协程里执行。协调者之外不读写 sched。
type operationFunc func(st *sched)

// sched 只存在于协调者协程。running 是唯一的任务表。
type sched struct {
	size       int
	threshold  time.Duration
	inspecting bool

	closed   bool
	wantExit bool
	nextID   uint64
	idle     int
	runningN int
	waitingN int
	// submitted、completed、failed、alerted 只在这一个协调者里累加。
	submitted uint64
	completed uint64
	failed    uint64
	alerted   uint64

	queue     taskQueue
	waitCount map[int]int
	running   map[uint64]*task

	// ready 里的任务已经标成 Running，但要等当前任务把 Result 送出后才启动。
	ready []*task
	// launch 里的任务在本次操作返回后由协调者启动。
	launch     []*task
	goroutines int

	inspectorOut bool
	closeWaiters []chan struct{}

	pool *Pool
}

func newSched(p *Pool) *sched {
	return &sched{
		size:       p.size,
		threshold:  p.threshold,
		inspecting: p.inspectWake != nil,
		idle:       p.size,
		nextID:     1,
		waitCount:  make(map[int]int),
		running:    make(map[uint64]*task),
		pool:       p,
	}
}

// run 是协调者。running 和全部计数只在这里读写。
func (p *Pool) run() {
	st := newSched(p)
	defer close(p.exited)
	for {
		op, ok := p.nextOp()
		if !ok {
			return
		}
		op(st)
		p.startTasks(st)
		p.pending.Add(-1)
	}
}

func (p *Pool) nextOp() (operationFunc, bool) {
	for {
		if p.terminal.Load() != nil && p.pending.Load() == 0 {
			return nil, false
		}
		select {
		case op := <-p.operationFuncChan:
			return op, true
		case <-p.wake:
		}
	}
}

// operate 把操作交给协调者并等它执行完。协调者已经退出时返回 false。
func (p *Pool) operate(fn operationFunc) bool {
	if p.terminal.Load() != nil {
		return false
	}
	p.pending.Add(1)
	if p.terminal.Load() != nil {
		p.dropPending()
		return false
	}
	done := make(chan struct{})
	select {
	case p.operationFuncChan <- func(st *sched) {
		defer close(done)
		fn(st)
	}:
		<-done
		return true
	case <-p.exited:
		p.dropPending()
		return false
	}
}

func (p *Pool) dropPending() {
	if p.pending.Add(-1) == 0 {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
}

// startTasks 在协调者上启动任务协程。调用时协调者不在接收操作，
// 新协程若立刻回来发送，会堵住直到本轮操作结束，不会和当前操作重入。
func (p *Pool) startTasks(st *sched) {
	jobs := st.launch
	st.launch = nil
	for _, job := range jobs {
		go p.execute(job)
	}
}

func (st *sched) addClose(waiter chan struct{}) {
	if st.wantExit {
		close(waiter)
		return
	}
	if !st.closed {
		st.closed = true
		st.pool.wakeInspector()
	}
	st.closeWaiters = append(st.closeWaiters, waiter)
	st.maybeCompleteClose()
}

// maybeCompleteClose 在任务都已送出结果、巡检也已退出时发布最终快照。
// 最终快照先于关闭等待通道发布，Close 返回之后的 Submit 读到的 Idle 是 0。
func (st *sched) maybeCompleteClose() {
	if st.wantExit || !st.closed {
		return
	}
	if st.runningN != 0 || st.waitingN != 0 || st.goroutines != 0 {
		return
	}
	if st.inspecting && !st.inspectorOut {
		return
	}
	st.idle = 0
	st.wantExit = true
	snap := st.snapshot(time.Now())
	st.pool.terminal.Store(&snap)
	for _, waiter := range st.closeWaiters {
		close(waiter)
	}
	st.closeWaiters = nil
}

// accept 在协调者里接受任务，或在已关闭时返回实况快照。
func (st *sched) accept(job *task, now time.Time) (Snapshot, bool) {
	if st.closed {
		return st.snapshot(now), true
	}
	job.accepted = now
	job.id = st.nextID
	st.nextID++
	st.submitted++
	if st.idle > 0 {
		st.idle--
		st.markRunning(job, now)
		st.launch = append(st.launch, job)
		st.goroutines++
		return Snapshot{}, false
	}
	heap.Push(&st.queue, job)
	st.waitingN++
	st.waitCount[job.priority]++
	return Snapshot{}, false
}

// finish 在协调者里计入终态、把下一个排队任务标成 Running，并复制快照。
// 队列非空时不增加 Idle。任务协程要等 release 才真正启动下一个用户函数。
func (st *sched) finish(job *task, now time.Time) Snapshot {
	delete(st.running, job.id)
	st.runningN--
	st.completed++
	if job.err != nil {
		st.failed++
	}
	if st.queue.Len() > 0 {
		next := heap.Pop(&st.queue).(*task)
		st.waitingN--
		st.waitCount[next.priority]--
		if st.waitCount[next.priority] == 0 {
			delete(st.waitCount, next.priority)
		}
		st.markRunning(next, now)
		st.ready = append(st.ready, next)
	} else {
		st.idle++
	}
	if st.closed && st.runningN == 0 && st.waitingN == 0 {
		st.pool.wakeInspector()
	}
	return st.snapshot(now)
}

// release 在 Result 已经送出后调用。这时才让协调者启动下一个任务协程。
func (st *sched) release() {
	st.goroutines--
	if len(st.ready) > 0 {
		next := st.ready[0]
		st.ready[0] = nil
		st.ready = st.ready[1:]
		st.launch = append(st.launch, next)
		st.goroutines++
	}
	st.maybeCompleteClose()
}

func (st *sched) markRunning(job *task, now time.Time) {
	job.started = now
	job.waitingFor = now.Sub(job.accepted)
	st.runningN++
	st.running[job.id] = job
}

func (st *sched) snapshot(now time.Time) Snapshot {
	snap := Snapshot{
		Size:      st.size,
		Idle:      st.idle,
		Running:   st.runningN,
		Waiting:   st.waitingN,
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
