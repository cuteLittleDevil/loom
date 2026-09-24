# ROADMAP

## 当前状态

协程池已按 `docs/design/pool.md` 实现。调度没有互斥锁，也没有 `Close`。`Submit` 把接受操作放进容量 100 的缓冲，缓冲满才另开协程发送。对外入口在 `pool.go`，协调者、执行者和告警分别在 `coordinator.go`、`executor.go`、`alert.go`。`go.mod` 要求 Go 1.27，toolchain 为 go1.27.1。设计文档的 Status 仍是 Draft。

## 进行中

无。

## 待确认

无。

## 最近完成

- 2026-09-24 12:29 README 只保留三条出路的总图，`Submit`、协调者、池内终态、池外执行和巡检挪到 `docs/flow.md`，README 与设计文档链到该文件。`README.md` `docs/flow.md` `docs/design/pool.md`
- 2026-09-24 12:24 README 的流程改为 5 张 Mermaid 图：整体、`Submit`、协调者、池内终态与池外执行、巡检。设计文档改为指向该节。`README.md` `docs/design/pool.md`
- 2026-09-23 14:49 README 使用示例补上 `Size`、告警、`Degrade`、`sign` 和 `loom.Result` 的注释。`README.md`
- 2026-09-23 14:48 README 使用示例改为提交自定义结构体 `Result{Name}`，`got.Value` 即该结构体。`README.md`
- 2026-09-23 14:42 例子、README 和设计文档里的日志改为 `log/slog`。`example/` `README.md` `docs/design/pool.md` `pool_test.go`
- 2026-09-23 14:40 增加 `example/basic`、`example/priority`、`example/degrade`、`example/alert`，README 写上运行命令。`README.md`
- 2026-09-23 14:35 补充提交流程图 `docs/flow.svg`，并按当前实现重写 `README.md`。`docs/design/pool.md`
- 2026-09-23 14:18 增加 `Config.Degrade`。池满时把当前运行任务交给它判断：返回 true 则另开协程直接执行，不占 `Size`；返回 false 则照旧排队。`pool.go` `coordinator.go` `pool_test.go` `docs/design/pool.md` `README.md`
- 2026-09-23 14:10 `Submit` 增加 `sign` 参数，作为来源标记写入 `TaskInfo` 和 `Alert`，不参与调度。`pool.go` `coordinator.go` `alert.go` `executor.go` `pool_test.go` `docs/design/pool.md` `README.md`
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

- 2026-09-24 12:29 核对 README 只保留总图并链接 `docs/flow.md`，四张详情图只出现在 `docs/flow.md`，设计文档指向这两处。图的分支沿用 12:24 的对照，未改调度代码，未重跑测试。
- 2026-09-24 12:24 对照 `pool.go` 的 `Submit` 与 `New`、`coordinator.go` 的 accept、降级、finish、release，以及 `alert.go` 的巡检，核对 README 中 5 张 Mermaid 图的分支。未改调度代码，未重跑测试。
- 2026-09-23 14:49 README 示例只增加注释，未改可执行语句，沿用 14:48 的 `go run` 结果。
- 2026-09-23 14:48 将 README 使用示例抽到临时模块 `go run`，输出 `result name=ok`。
- 2026-09-23 14:42 四个 example 改为 `slog` 后重新 `go run`，分别打出 result、order `[10 5 1]`、degraded、alert。`go test -count=1 -run TestUnreadResultDoesNotBlockWorker` 通过。
- 2026-09-23 14:40 `go run ./example/basic` 输出 `value=ok`；`go run ./example/priority` 输出 `order= 10 5 1`；`go run ./example/degrade` 输出池外执行且 `submitted=1`；`go run ./example/alert` 打出 `sign=slow`。`go test -count=1 ./...` 通过。
- 2026-09-23 14:35 对照 `coordinator.go` 的接受、降级、finish、release 和 `alert.go` 的巡检，核对 `docs/flow.svg` 与 `README.md` 的分支一致。未改调度代码，未重跑测试。
- 2026-09-23 14:18 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。池满且 `Degrade` 返回 true 时任务在池外执行；返回 false 时继续排队；有空槽位时不调用 `Degrade`。
- 2026-09-23 14:10 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。运行中任务的 `Sign` 和告警的 `Sign` 与提交时一致。
- 2026-09-23 14:02 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。计数改为现算后，快照不变量、优先级和告警仍然通过。
- 2026-09-23 13:50 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。缓冲投递后，优先级顺序、并发上限和告警仍然通过。
- 2026-09-23 13:42 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。去掉 `Close` 后，调度、告警和并发上限仍然通过。
- 2026-09-23 13:19 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。拆文件和重命名后，原有调度、告警、`Close`、并发上限仍然通过。
- 2026-09-23 13:09 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout 180s ./...` 通过；`go vet ./...` 通过。覆盖原有调度、告警、`Close`，以及并发上限和 `Close` 与 `Submit` 交错。
- 2026-09-23 11:54 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 ./...` 通过；`go vet ./...` 通过。
- 2026-09-23 11:33 通读修订后的 `docs/design/pool.md` ，检索残留的阻塞返回和排队取消。结果：`context` 只出现在「池子不提供」的说明里；调度不变量、计数器和 `Close` 与新提交口一致。代码未实现，没有运行时测试。
- 2026-09-23 11:28 通读修订后的 `docs/design/pool.md` ，对照调度不变量、计数器表、取消分界、`Close` 和告警相位检查前后表述。结果：这些规则互相一致；代码未实现，没有运行时测试。
