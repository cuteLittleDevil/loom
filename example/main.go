package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/cuteLittleDevil/loom"
)

// Result 是业务自己的结构体。Submit 的类型参数 R 由函数返回值推断，调用方不用写出 [R]。
type Result struct {
	Name string
}

func main() {
	// Size 是同时执行的用户函数上限，必须大于 0。
	// OccupyThreshold 大于等于 0 才启动巡检。任务还在跑且超过 30s 时调用 OnAlert，回调在协调者之外。
	// 8 个槽位都忙，并且每个任务都已运行至少 30s 时，Degrade 返回 true，这次提交改到池外执行，不占 Size。
	p, err := loom.New(loom.Config{
		Size:            8,
		OccupyThreshold: 30 * time.Second,
		OnAlert: func(a loom.Alert) {
			slog.Info("occupy", "sign", a.Sign, "id", a.TaskID, "running", a.RunningFor, "waiting", a.Waiting)
		},
		Degrade: func(running []loom.TaskInfo) bool {
			if len(running) < 8 {
				return false
			}
			for _, task := range running {
				if task.RunningFor < 30*time.Second {
					return false
				}
			}
			return true
		},
	})
	if err != nil {
		slog.Error("create pool", "err", err)
		os.Exit(1)
	}

	// sign 是来源标记，不参与优先级和任务 ID 排序。WithPriority 数值更大的先执行。
	// Submit 立刻返回容量为 1 的 channel，不等待函数结束。
	ch := p.Submit("sign", func() (Result, error) {
		return Result{Name: "ok"}, nil
	}, loom.WithPriority(10))
	// got 的类型是 loom.Result[Result]。Value 是上面的业务结构体，Snapshot 是终态那一刻的池子。
	got := <-ch
	if got.Err != nil {
		slog.Error("task", "err", got.Err)
		os.Exit(1)
	}
	slog.Info("result", "name", got.Value.Name)
}
