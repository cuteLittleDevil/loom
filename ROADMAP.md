# ROADMAP

## 当前状态

协程池已按 `docs/design/pool.md` 实现。`go.mod` 要求 Go 1.27，toolchain 为 go1.27.1。设计文档的 Status 仍是 Draft。

## 进行中

无。

## 待确认

无。

## 最近完成

- 2026-09-23 11:54 按修订后的设计实现协程池：泛型方法 `Submit`、优先级队列、占用告警和 `Close`。`pool.go` `errors.go` `pool_test.go`
- 2026-09-23 11:33 把 `Submit` 改成泛型方法，返回 `<-chan Result[R]`，去掉 `context` 和排队取消。`docs/design/pool.md`
- 2026-09-23 11:28 修订协程池设计草稿，写明单锁分派、取消与开始执行的分界、计数器、错误与快照、`Close` 的等待范围和告警合并相位。`docs/design/pool.md`
- 2026-09-23 11:00 写下协程池设计草稿 `docs/design/pool.md`

## 最近验证

- 2026-09-23 11:54 `GOTOOLCHAIN=go1.27.1 go test -race -count=1 ./...` 通过；`go vet ./...` 通过。
- 2026-09-23 11:33 通读修订后的 `docs/design/pool.md` ，检索残留的阻塞返回和排队取消。结果：`context` 只出现在「池子不提供」的说明里；调度不变量、计数器和 `Close` 与新提交口一致。代码未实现，没有运行时测试。
- 2026-09-23 11:28 通读修订后的 `docs/design/pool.md` ，对照调度不变量、计数器表、取消分界、`Close` 和告警相位检查前后表述。结果：这些规则互相一致；代码未实现，没有运行时测试。
