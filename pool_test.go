package loom_test

import (
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cuteLittleDevil/loom"
)

func mustPool(t *testing.T, cfg loom.Config) *loom.Pool {
	t.Helper()
	p, err := loom.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return p
}

func recv[R any](t *testing.T, ch <-chan loom.Result[R]) loom.Result[R] {
	t.Helper()
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before result")
		}
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for result")
		return loom.Result[R]{}
	}
}

func TestInvalidSize(t *testing.T) {
	before := runtime.NumGoroutine()
	for _, size := range []int{0, -1} {
		p, err := loom.New(loom.Config{Size: size})
		if p != nil || !errors.Is(err, loom.ErrInvalidSize) {
			t.Fatalf("size %d: pool=%v err=%v", size, p, err)
		}
	}
	if got := runtime.NumGoroutine(); got != before {
		t.Fatalf("goroutines before=%d after=%d", before, got)
	}
}

func TestSubmitReturnsBeforeFnDone(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 2, OccupyThreshold: -1})
	release := make(chan struct{})
	entered := make(chan struct{})
	returned := make(chan struct{})
	var ch <-chan loom.Result[int]
	go func() {
		ch = p.Submit(func() (int, error) {
			close(entered)
			<-release
			return 7, nil
		})
		close(returned)
	}()
	<-entered
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Submit blocked until fn returned")
	}
	close(release)
	got := recv(t, ch)
	if got.Value != 7 || got.Err != nil {
		t.Fatalf("value=%d err=%v", got.Value, got.Err)
	}
	if len(got.Snapshot.RunningTasks) != 0 {
		t.Fatalf("finished task still running: %+v", got.Snapshot.RunningTasks)
	}
	if got.Snapshot.Idle+got.Snapshot.Running != got.Snapshot.Size || got.Snapshot.Idle < 1 {
		t.Fatalf("snapshot: %+v", got.Snapshot)
	}
	if _, ok := <-ch; ok {
		t.Fatal("second receive ok=true")
	}
}

func TestUnreadResultDoesNotBlockWorker(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	p.Submit(func() (int, error) { return 1, nil })
	ch := p.Submit(func() (int, error) { return 2, nil })
	got := recv(t, ch)
	if got.Value != 2 || got.Err != nil {
		t.Fatalf("value=%d err=%v", got.Value, got.Err)
	}
}

func TestPriorityOrder(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	release := make(chan struct{})
	entered := make(chan struct{})
	hold := p.Submit(func() (int, error) {
		close(entered)
		<-release
		return 0, nil
	})
	<-entered

	order := make(chan int, 3)
	submit := func(priority int) <-chan loom.Result[int] {
		return p.Submit(func() (int, error) {
			order <- priority
			return priority, nil
		}, loom.WithPriority(priority))
	}
	ch1 := submit(1)
	ch10 := submit(10)
	ch5 := submit(5)
	time.Sleep(20 * time.Millisecond)
	close(release)

	got := recv(t, hold)
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Snapshot.Idle != 0 || got.Snapshot.Waiting != 2 {
		t.Fatalf("snapshot: %+v", got.Snapshot)
	}
	if len(got.Snapshot.RunningTasks) != 1 || got.Snapshot.RunningTasks[0].Priority != 10 {
		t.Fatalf("running: %+v", got.Snapshot.RunningTasks)
	}
	if got.Snapshot.RunningTasks[0].WaitingFor < 10*time.Millisecond {
		t.Fatalf("WaitingFor=%s", got.Snapshot.RunningTasks[0].WaitingFor)
	}
	if got.Snapshot.WaitingBy[0].Priority != 5 || got.Snapshot.WaitingBy[1].Priority != 1 {
		t.Fatalf("WaitingBy: %+v", got.Snapshot.WaitingBy)
	}

	var gotOrder []int
	for range 3 {
		select {
		case priority := <-order:
			gotOrder = append(gotOrder, priority)
		case <-time.After(2 * time.Second):
			t.Fatalf("start order timeout, got %v", gotOrder)
		}
	}
	if !slices.Equal(gotOrder, []int{10, 5, 1}) {
		t.Fatalf("start order %v", gotOrder)
	}
	for _, ch := range []<-chan loom.Result[int]{ch1, ch10, ch5} {
		if got := recv(t, ch); got.Err != nil {
			t.Fatal(got.Err)
		}
	}
}

func TestSamePriorityUsesTaskID(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	release := make(chan struct{})
	entered := make(chan struct{})
	hold := p.Submit(func() (int, error) {
		close(entered)
		<-release
		return 0, nil
	})
	<-entered
	order := make(chan int, 3)
	for id := 1; id <= 3; id++ {
		id := id
		p.Submit(func() (int, error) {
			order <- id
			return id, nil
		})
	}
	close(release)
	recv(t, hold)
	var gotOrder []int
	for range 3 {
		gotOrder = append(gotOrder, <-order)
	}
	if !slices.Equal(gotOrder, []int{1, 2, 3}) {
		t.Fatalf("start order %v", gotOrder)
	}
}

func TestLastWithPriorityWins(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	release := make(chan struct{})
	entered := make(chan struct{})
	hold := p.Submit(func() (string, error) {
		close(entered)
		<-release
		return "", nil
	})
	<-entered
	order := make(chan string, 2)
	p.Submit(func() (int, error) {
		order <- "low"
		return 0, nil
	}, loom.WithPriority(9), nil, loom.WithPriority(1))
	p.Submit(func() (int, error) {
		order <- "high"
		return 0, nil
	}, loom.WithPriority(4))
	close(release)
	recv(t, hold)
	if got := <-order; got != "high" {
		t.Fatal(got)
	}
	if got := <-order; got != "low" {
		t.Fatal(got)
	}
}

func TestFuncError(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	sentinel := errors.New("biz")
	got := recv(t, p.Submit(func() (int, error) { return 4, sentinel }))
	if got.Value != 4 || got.Err != sentinel {
		t.Fatalf("value=%d err=%v", got.Value, got.Err)
	}
	if got.Snapshot.Submitted != 1 || got.Snapshot.Completed != 1 || got.Snapshot.Failed != 1 {
		t.Fatalf("snapshot: %+v", got.Snapshot)
	}
}

func TestPanicRecovered(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	got := recv(t, p.Submit(func() (int, error) { panic("boom") }))
	if got.Value != 0 || !errors.Is(got.Err, loom.ErrPanic) {
		t.Fatalf("value=%d err=%v", got.Value, got.Err)
	}
	var pe *loom.PanicError
	if !errors.As(got.Err, &pe) || pe.Value != "boom" || len(pe.Stack) == 0 {
		t.Fatalf("panic error %#v", pe)
	}
	if got.Snapshot.Failed != 1 || got.Snapshot.Completed != 1 {
		t.Fatalf("snapshot: %+v", got.Snapshot)
	}
	next := recv(t, p.Submit(func() (string, error) { return "next", nil }))
	if next.Value != "next" || next.Err != nil {
		t.Fatalf("value=%s err=%v", next.Value, next.Err)
	}
}

func TestNilRejectsDoNotTouchCounters(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	base := recv(t, p.Submit(func() (int, error) { return 1, nil })).Snapshot

	ch := p.Submit[int](nil)
	assertBuffered(t, ch, loom.ErrNilFunc)

	after := recv(t, p.Submit(func() (int, error) { return 2, nil })).Snapshot
	if after.Submitted != base.Submitted+1 || after.Completed != base.Completed+1 || after.Failed != base.Failed {
		t.Fatalf("base=%+v after=%+v", base, after)
	}

	assertBuffered(t, (*loom.Pool)(nil).Submit(func() (int, error) { return 1, nil }), loom.ErrNilPool)
	assertBuffered(t, (*loom.Pool)(nil).Submit[int](nil), loom.ErrNilPool)
	if err := (*loom.Pool)(nil).Close(); !errors.Is(err, loom.ErrNilPool) {
		t.Fatal(err)
	}
}

func assertBuffered[R comparable](t *testing.T, ch <-chan loom.Result[R], want error) {
	t.Helper()
	select {
	case got := <-ch:
		if !errors.Is(got.Err, want) {
			t.Fatalf("err=%v", got.Err)
		}
		if !zeroSnapshot(got.Snapshot) {
			t.Fatalf("snapshot %+v", got.Snapshot)
		}
		var zero R
		if got.Value != zero {
			t.Fatalf("value=%v", got.Value)
		}
	default:
		t.Fatal("result was not buffered before Submit returned")
	}
	if _, ok := <-ch; ok {
		t.Fatal("second receive ok=true")
	}
}

func zeroSnapshot(s loom.Snapshot) bool {
	return s.Size == 0 && s.Idle == 0 && s.Running == 0 && s.Waiting == 0 &&
		len(s.WaitingBy) == 0 && len(s.RunningTasks) == 0 &&
		s.Submitted == 0 && s.Completed == 0 && s.Failed == 0 && s.Alerted == 0
}

func TestCloseDrainsThenRejects(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: -1})
	release := make(chan struct{})
	entered := make(chan struct{})
	hold := p.Submit(func() (int, error) {
		close(entered)
		<-release
		return 0, nil
	})
	<-entered
	queued := p.Submit(func() (int, error) { return 1, nil }, loom.WithPriority(1))

	errCh := make(chan error, 2)
	go func() { errCh <- p.Close() }()
	go func() { errCh <- p.Close() }()
	select {
	case err := <-errCh:
		t.Fatalf("Close returned before the running task finished: %v", err)
	case <-time.After(80 * time.Millisecond):
	}
	close(release)

	got := recv(t, hold)
	if got.Err != nil || got.Value != 0 {
		t.Fatalf("value=%d err=%v", got.Value, got.Err)
	}
	if got.Snapshot.Idle != 0 || got.Snapshot.Waiting != 0 || len(got.Snapshot.RunningTasks) != 1 || got.Snapshot.RunningTasks[0].Priority != 1 {
		t.Fatalf("hold snapshot: %+v running=%+v", got.Snapshot, got.Snapshot.RunningTasks)
	}
	queuedGot := recv(t, queued)
	if queuedGot.Err != nil || queuedGot.Value != 1 {
		t.Fatalf("queued value=%d err=%v", queuedGot.Value, queuedGot.Err)
	}
	if queuedGot.Snapshot.Idle < 1 {
		t.Fatalf("last result Idle=%d", queuedGot.Snapshot.Idle)
	}
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}

	ch := p.Submit(func() (int, error) { return 2, nil })
	select {
	case rejected := <-ch:
		if !errors.Is(rejected.Err, loom.ErrClosed) {
			t.Fatal(rejected.Err)
		}
		if rejected.Snapshot.Idle != 0 || rejected.Snapshot.Running != 0 || rejected.Snapshot.Waiting != 0 || rejected.Snapshot.Size != 1 {
			t.Fatalf("snapshot: %+v", rejected.Snapshot)
		}
		if rejected.Snapshot.Submitted != 2 || rejected.Snapshot.Completed != 2 {
			t.Fatalf("counters: %+v", rejected.Snapshot)
		}
	default:
		t.Fatal("ErrClosed was not buffered")
	}
}

func TestCloseExitsGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	p, err := loom.New(loom.Config{Size: 3, OccupyThreshold: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	waitGoroutines(t, before)
}

func TestCloseDoesNotWaitFullInspectInterval(t *testing.T) {
	p, err := loom.New(loom.Config{Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Close took %s", elapsed)
	}
}

func TestAlertWhileTaskStillRunning(t *testing.T) {
	const threshold = 30 * time.Millisecond
	alerts := make(chan loom.Alert, 4)
	p := mustPool(t, loom.Config{
		Size:            1,
		OccupyThreshold: threshold,
		OnAlert: func(a loom.Alert) {
			alerts <- a
		},
	})
	release := make(chan struct{})
	ch := p.Submit(func() (string, error) {
		<-release
		return "ok", nil
	})
	select {
	case alert := <-alerts:
		if alert.TaskID == 0 || alert.Running < 1 || alert.RunningFor < threshold {
			t.Fatalf("alert: %+v", alert)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no alert")
	}
	close(release)
	got := recv(t, ch)
	if got.Value != "ok" || got.Err != nil {
		t.Fatalf("value=%s err=%v", got.Value, got.Err)
	}
	if got.Snapshot.Alerted < 1 {
		t.Fatalf("Alerted=%d", got.Snapshot.Alerted)
	}
}

func TestShortTaskCanSkipAlert(t *testing.T) {
	var calls atomic.Int32
	p := mustPool(t, loom.Config{
		Size:            1,
		OccupyThreshold: time.Second,
		OnAlert:         func(loom.Alert) { calls.Add(1) },
	})
	recv(t, p.Submit(func() (int, error) { return 1, nil }))
	time.Sleep(40 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestNilOnAlertStillCounts(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 1, OccupyThreshold: 20 * time.Millisecond})
	got := recv(t, p.Submit(func() (int, error) {
		time.Sleep(120 * time.Millisecond)
		return 1, nil
	}))
	if got.Err != nil || got.Snapshot.Alerted < 1 {
		t.Fatalf("err=%v alerted=%d", got.Err, got.Snapshot.Alerted)
	}
}

func TestAlertMergesSkippedPeriods(t *testing.T) {
	const threshold = 40 * time.Millisecond
	var count atomic.Int32
	sawFirst := make(chan struct{})
	releaseInspector := make(chan struct{})
	sawSecond := make(chan struct{})
	p := mustPool(t, loom.Config{
		Size:            1,
		OccupyThreshold: threshold,
		OnAlert: func(loom.Alert) {
			n := count.Add(1)
			switch n {
			case 1:
				close(sawFirst)
				<-releaseInspector
			case 2:
				close(sawSecond)
			}
		},
	})
	releaseTask := make(chan struct{})
	ch := p.Submit(func() (int, error) {
		<-releaseTask
		return 1, nil
	})
	select {
	case <-sawFirst:
	case <-time.After(2 * time.Second):
		t.Fatal("missing first alert")
	}
	time.Sleep(3 * threshold)
	close(releaseInspector)
	select {
	case <-sawSecond:
	case <-time.After(2 * time.Second):
		t.Fatal("missing merged alert")
	}
	if got := count.Load(); got != 2 {
		t.Fatalf("alerts=%d", got)
	}
	close(releaseTask)
	got := recv(t, ch)
	if got.Value != 1 || got.Err != nil || got.Snapshot.Alerted != 2 {
		t.Fatalf("value=%d err=%v alerted=%d", got.Value, got.Err, got.Snapshot.Alerted)
	}
}

func TestOnAlertPanicContinues(t *testing.T) {
	sawOther := make(chan struct{})
	p := mustPool(t, loom.Config{
		Size:            2,
		OccupyThreshold: 20 * time.Millisecond,
		OnAlert: func(a loom.Alert) {
			if a.TaskID == 1 {
				panic("alert boom")
			}
			if a.TaskID == 2 {
				select {
				case <-sawOther:
				default:
					close(sawOther)
				}
			}
		},
	})
	release := make(chan struct{})
	entered := make(chan struct{}, 2)
	fn := func() (int, error) {
		entered <- struct{}{}
		<-release
		return 1, nil
	}
	p.Submit(fn)
	p.Submit(fn)
	<-entered
	<-entered
	select {
	case <-sawOther:
	case <-time.After(2 * time.Second):
		t.Fatal("inspector stopped after OnAlert panic")
	}
	close(release)
}

func TestConcurrentSubmit(t *testing.T) {
	p := mustPool(t, loom.Config{Size: 4, OccupyThreshold: -1})
	const n = 100
	var wg sync.WaitGroup
	var sum atomic.Int64
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			ch := p.Submit(func() (int, error) {
				return i, nil
			}, loom.WithPriority(i%5-2))
			select {
			case got := <-ch:
				if got.Err != nil || got.Value != i {
					t.Errorf("task %d: value=%d err=%v", i, got.Value, got.Err)
					return
				}
				checkSnapshot(t, got.Snapshot)
				sum.Add(int64(i))
			case <-time.After(3 * time.Second):
				t.Errorf("task %d timed out", i)
			}
		}()
	}
	wg.Wait()
	if sum.Load() != int64(n*(n-1)/2) {
		t.Fatalf("sum=%d", sum.Load())
	}
}

func checkSnapshot(t *testing.T, snap loom.Snapshot) {
	t.Helper()
	if snap.Waiting > 0 && snap.Idle != 0 {
		t.Errorf("Waiting=%d Idle=%d", snap.Waiting, snap.Idle)
	}
	if snap.Idle+snap.Running != snap.Size {
		t.Errorf("Idle+Running=%d Size=%d", snap.Idle+snap.Running, snap.Size)
	}
	if snap.Running+snap.Waiting != int(snap.Submitted-snap.Completed) {
		t.Errorf("inflight running=%d waiting=%d submitted=%d completed=%d",
			snap.Running, snap.Waiting, snap.Submitted, snap.Completed)
	}
	for i := 1; i < len(snap.RunningTasks); i++ {
		if snap.RunningTasks[i].ID < snap.RunningTasks[i-1].ID {
			t.Errorf("RunningTasks not sorted: %+v", snap.RunningTasks)
			break
		}
	}
	for i := 1; i < len(snap.WaitingBy); i++ {
		if snap.WaitingBy[i].Priority > snap.WaitingBy[i-1].Priority {
			t.Errorf("WaitingBy not sorted: %+v", snap.WaitingBy)
			break
		}
	}
}

func waitGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines before=%d after=%d", before, runtime.NumGoroutine())
		}
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
}
