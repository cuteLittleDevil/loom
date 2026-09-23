package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/cuteLittleDevil/loom"
)

func main() {
	alerts := make(chan loom.Alert, 1)
	p, err := loom.New(loom.Config{
		Size:            1,
		OccupyThreshold: 40 * time.Millisecond,
		OnAlert: func(a loom.Alert) {
			select {
			case alerts <- a:
			default:
			}
		},
	})
	if err != nil {
		slog.Error("create pool", "err", err)
		os.Exit(1)
	}
	release := make(chan struct{})
	ch := p.Submit("slow", func() (string, error) {
		<-release
		return "done", nil
	})
	alert := <-alerts
	slog.Info("alert", "sign", alert.Sign, "running", alert.RunningFor, "idle", alert.Idle)
	close(release)
	<-ch
}
