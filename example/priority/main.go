package main

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/cuteLittleDevil/loom"
)

func main() {
	p, err := loom.New(loom.Config{Size: 1, OccupyThreshold: -1})
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

	order := make(chan int, 3)
	submit := func(priority int) {
		p.Submit("p"+strconv.Itoa(priority), func() (int, error) {
			order <- priority
			return priority, nil
		}, loom.WithPriority(priority))
	}
	submit(1)
	submit(10)
	submit(5)
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-hold

	got := make([]int, 0, 3)
	for range 3 {
		got = append(got, <-order)
	}
	slog.Info("order", "priorities", got)
}
