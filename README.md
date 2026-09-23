# loom

固定容量的 Go 协程池。`Submit` 立刻返回结果 channel，池满时按优先级排队，worker 占用超时只告警。

要求 Go 1.27 或更新。设计见 [docs/design/pool.md](docs/design/pool.md) 。

```go
p, err := loom.New(loom.Config{Size: 8})
if err != nil {
    return err
}
defer p.Close()

ch := p.Submit(func() (string, error) {
    return "ok", nil
})
got := <-ch
```
