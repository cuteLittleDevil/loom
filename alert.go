package loom

import (
	"sort"
	"time"
)

func (p *Pool) inspect() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !p.emitAlerts() {
				return
			}
		case <-p.inspectWake:
		}
		var done bool
		if !p.operate(func(st *sched) {
			done = st.closed && st.runningN == 0 && st.waitingN == 0
		}) {
			return
		}
		if done {
			p.operate(func(st *sched) {
				st.inspectorOut = true
				st.maybeCompleteClose()
			})
			return
		}
	}
}

// emitAlerts 让协调者合并跨过的周期，再按任务 ID 顺序调用 OnAlert。
func (p *Pool) emitAlerts() bool {
	var alerts []Alert
	if !p.operate(func(st *sched) {
		alerts = st.collectAlerts(time.Now())
	}) {
		return false
	}
	if p.onAlert == nil {
		return true
	}
	for _, alert := range alerts {
		func() {
			defer func() { recover() }()
			p.onAlert(alert)
		}()
	}
	return true
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
			Priority:   job.priority,
			RunningFor: elapsed,
			Idle:       st.idle,
			Running:    st.runningN,
			Waiting:    st.waitingN,
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
