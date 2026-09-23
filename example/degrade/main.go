package main

import (
	"log/slog"
	"os"

	"github.com/cuteLittleDevil/loom"
)

func main() {
	p, err := loom.New(loom.Config{
		Size:            1,
		OccupyThreshold: -1,
		Degrade: func(running []loom.TaskInfo) bool {
			return len(running) == 1 && running[0].Sign == "hold"
		},
	})
	if err != nil {
		slog.Error("create pool", "err", err)
		os.Exit(1)
	}
	release := make(chan struct{})
	entered := make(chan struct{})
	hold := p.Submit("hold", func() (string, error) {
		close(entered)
		<-release
		return "hold", nil
	})
	<-entered

	extra := p.Submit("extra", func() (string, error) {
		return "degraded", nil
	})
	got := <-extra
	slog.Info("result",
		"value", got.Value,
		"running", got.Snapshot.Running,
		"sign", got.Snapshot.RunningTasks[0].Sign,
		"submitted", got.Snapshot.Submitted,
	)
	close(release)
	<-hold
}
