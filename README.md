# loom

固定容量的 Go 协程池。`Submit` 立刻返回结果 channel，池满时按优先级排队，也可以用 `Degrade` 改成池外直接执行。任务占用超时只告警。调度只发生在协调者协程里，池内没有互斥锁。

要求 Go 1.27 或更新。设计见 [docs/design/pool.md](docs/design/pool.md) 。

```go
p, err := loom.New(loom.Config{Size: 8})
if err != nil {
    return err
}

ch := p.Submit("billing", func() (string, error) {
    return "ok", nil
})
got := <-ch
```
