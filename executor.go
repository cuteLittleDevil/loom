package loom

import "time"

// task 是一次已接受的提交。exec 与 deliver 由 Submit 按 R 闭包填充。
type task struct {
	id         uint64
	sign       string
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

// execute 在任务协程里跑用户函数，再回到协调者取终态快照。
// 下一个任务要等 Result 送出后才启动，这样同时执行的用户函数不超过 Size。
func (p *Pool) execute(job *task) {
	job.exec()
	var snap Snapshot
	p.operate(func(st *sched) {
		snap = st.finish(job, time.Now())
	})
	job.deliver(snap)
	p.operate(func(st *sched) {
		st.release()
	})
}
