package loom

import (
	"sort"
	"time"
)

func (p *Pool) inspect() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for range ticker.C {
		p.emitAlerts()
	}
}

// emitAlerts 让协调者合并跨过的周期，再按任务 ID 顺序调用 OnAlert。
func (p *Pool) emitAlerts() {
	var alerts []Alert
	p.operate(func(st *sched) {
		alerts = st.collectAlerts(time.Now())
	})
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

func (st *sched) collectAlerts(now time.Time) []Alert {
	if st.threshold <= 0 || len(st.running) == 0 {
		return nil
	}
	jobs := make([]*task, 0, len(st.running))
	for _, job := range st.running {
		jobs = append(jobs, job)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].id < jobs[j].id })

	idle, running, waiting := st.counts()
	var alerts []Alert
	for _, job := range jobs {
		elapsed := now.Sub(job.started)
		periods := int64(elapsed / st.threshold)
		if periods <= job.alertK {
			continue
		}
		job.alertK = periods
		job.alerted = true
		st.alerted++
		alerts = append(alerts, Alert{
			TaskID:     job.id,
			Sign:       job.sign,
			Priority:   job.priority,
			RunningFor: elapsed,
			Idle:       idle,
			Running:    running,
			Waiting:    waiting,
			At:         now,
		})
	}
	return alerts
}
