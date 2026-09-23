# ROADMAP

## 当前状态

协程池已按 `docs/design/pool.md` 实现。调度没有互斥锁，也没有 `Close`。`Submit` 把接受操作放进容量 100 的缓冲，缓冲满才另开协程发送。对外入口在 `pool.go`，协调者、执行者和告警分别在 `coordinator.go`、`executor.go`、`alert.go`。`go.mod` 要求 Go 1.27，toolchain 为 go1.27.1。设计文档的 Status 仍是 Draft。

## 进行中

无。

## 待确认

无。

## 最近完成

- 2026-09-23 14:02 删掉协调者里无人读取的 `goroutines`。`Idle`、`Running`、`Waiting` 改为从运行表和队列现算，不再另记一份。`coordinator.go` `alert.go`
- 2026-09-23 13:50 `Submit` 的接受操作改成容量 100 的缓冲；缓冲满时才另开协程阻塞发送，调用方不再等待协调者。`pool.go` `coordinator.go` `docs/design/pool.md`
- 2026-09-23 13:42 去掉池的 `Close`、`ErrClosed` 和只为退出服务的最终快照。协调者和巡检不再退出。`pool.go` `coordinator.go` `executor.go` `alert.go` `errors.go` `pool_test.go` `docs/design/pool.md` `README.md`
- 2026-09-23 13:19 按协调者、执行者、告警拆成 `coordinator.go`、`executor.go`、`alert.go`。`*task` 变量改名为 `job`，避免和结构体 `task` 重名。`pool.go` `docs/design/pool.md`
- 2026-09-23 13:09 去掉池内互斥锁，改成协调者串行调度：运行表和计数只在协调者里修改，协调者按空闲槽位启动任务协程，任务结束后回到协调者取快照。`pool.go` `pool_test.go` `docs/design/pool.md` `README.md`
- 2026-09-23 11:54 按修订后的设计实现协程池：泛型方法 `Submit`、优先级队列、占用告警和 `Close`。`pool.go` `errors.go` `pool_test.go`
- 2026-09-23 11:33 把 `Submit` 改成泛型方法，返回 `<-chan Result[R]`，去掉 `context` 和排队取消。`docs/design/pool.md`
- 2026-09-23 11:28 修订协程池设计草稿，写明单锁分派、取消与开始执行的分界、计数器、错误与快照、`Close` 的等待范围和告警合并相位。`docs/design/pool.md`
- 2026-09-23 11:00 写下协程池设计草稿 `docs/design/pool.md`

## 最近验证

- 2026-09-23 14:02 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。计数改为现算后，快照不变量、优先级和告警仍然通过。
- 2026-09-23 13:50 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。缓冲投递后，优先级顺序、并发上限和告警仍然通过。
- 2026-09-23 13:42 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。去掉 `Close` 后，调度、告警和并发上限仍然通过。
- 2026-09-23 13:19 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。拆文件和重命名后，原有调度、告警、`Close`、并发上限仍然通过。
- 2026-09-23 13:09 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。覆盖原有调度、告警、`Close`，以及并发上限和 `Close` 与 `Submit` 交错。
- 2026-09-23 11:54 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 ./...` 通过；`go vet ./...` 通过。
- 2026-09-23 11:33 通读修订后的 `docs/design/pool.md` ，检索残留的阻塞返回和排队取消。结果：`context` 只出现在「池子不提供」的说明里；调度不变量、计数器和 `Close` 与新提交口一致。代码未实现，没有运行时测试。
- 2026-09-23 11:28 通读修订后的 `docs/design/pool.md` ，对照调度不变量、计数器表、取消分界、`Close` 和告警相位检查前后表述。结果：这些规则互相一致；代码未实现，没有运行时测试。
