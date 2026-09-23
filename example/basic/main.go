package main

import (
	"log/slog"
	"os"

	"github.com/cuteLittleDevil/loom"
)

func main() {
	p, err := loom.New(loom.Config{Size: 2, OccupyThreshold: -1})
	if err != nil {
		slog.Error("create pool", "err", err)
		os.Exit(1)
	}
	ch := p.Submit("billing", func() (string, error) {
		return "ok", nil
	})
	got := <-ch
	slog.Info("result",
		"value", got.Value,
		"err", got.Err,
		"submitted", got.Snapshot.Submitted,
		"completed", got.Snapshot.Completed,
	)
}
