Title: 协调者按固定并发执行泛型任务，池满按优先级等待，占用超时只告警
Author(s): cd
Last updated: 2026-09-23
Discussion: 仓库内评审本文件，确认前不写实现
Status: Draft

## Abstract / 摘要

`loom` 提供一个只依赖标准库的固定容量协程池。`Submit` 是 `Pool` 上的泛型方法，立刻返回 `<-chan Result[R]`，不阻塞到任务结束。用户函数的形状是 `func() (R, error)`，只在协调者启动的任务协程上执行。channel 容量为 1，恰好送出一次 `Result` 然后关闭；没人接收也不会堵住任务协程。`Result` 里的快照是任务终态并完成分派的那一刻，不是调用方后来执行 `<-ch` 的那一刻。池满时任务进入优先级队列；调度只在协调者协程里完成，运行中的任务记在它的唯一 map 上，数值更大的优先级先执行，同优先级按任务 ID。正在执行的任务不会被抢占。任务连续执行超过阈值时，池子在任务仍被巡检看到的期间发出告警。最重要的承诺是：并发上限等于配置的槽位数，告警不改变任务的控制流。池内调度没有互斥锁。

## Background / 背景与动机

固定并发、带回返回值、还要知道池子里谁堵住了，这三件事用信号量拼不齐。

下面这段代码把并发限制在 8。它能跑，但返回值要靠外层变量偷运，等待顺序取决于谁先抢到 `sem`，调用方看不到哪条任务已经占住槽位。

```go
sem := make(chan struct{}, 8)
sem <- struct{}{}
defer func() { <-sem }()
v, err := work(ctx)
```

池子一旦被慢调用占满，后来的高优先级任务只能排在已经排队的低优先级任务后面。慢调用本身没有期限。若提交调用要阻塞到函数结束，等待期间调用方也拿不到池内快照。

这个问题可以定成一句：调用方要的是带优先级的有界并发。提交立刻拿回 channel，结果里带上终态那一刻的快照；占用告警在任务还在跑的时候发出。

## Design / Proposal / 设计

### 池子在创建时启动协调者，并发上限就是配置的槽位数

```go
p, err := loom.New(loom.Config{
    Size:            8,
    OccupyThreshold: 30 * time.Second,
    OnAlert: func(a loom.Alert) {
        log.Printf("task %d priority %d running %s, waiting %d",
            a.TaskID, a.Priority, a.RunningFor, a.Waiting)
    },
})
if err != nil {
    return err
}
```

`Size` 是同时执行的用户函数上限。`New` 成功时立刻启动一个协调者 goroutine，不为排队任务预开协程。有空闲槽位时，协调者再启动任务协程去执行用户函数；槽位用完就只进优先级队列。告警开启时再启动一个巡检 goroutine。协调者和巡检随进程存在，池子不提供 `Close`。`Size <= 0` 时 `New` 返回 `nil, ErrInvalidSize`，不启动任何 goroutine。

`OccupyThreshold` 是单次任务的占用告警线。零值按 30s 处理。负值表示不启动巡检、不告警。正值即使小于 1s 也照用，池子不另设最小阈值。`OnAlert` 可以为 nil：计数和 `TaskInfo.Alerted` 仍然更新，只是没有回调。

调度状态只放在协调者协程里。运行中的任务在它的唯一 `map[uint64]*task` 上，各优先级的排队数量记在同一协调者的 `waitCount` 里，提交、完成、失败和告警次数也只由它修改。用户函数和 `OnAlert` 都在协调者之外执行。`Submit`、巡检和回调可以并发，它们把操作发给协调者，由协调者串行执行。

`Submit` 把接受操作放进容量为 100 的缓冲后就返回，不等协调者执行，也不等用户函数结束。缓冲满时才另开一条协程去做这次发送，调用方仍然返回。在回调或用户函数里调用它不会占住任务协程。用户函数里接收同一个池的 `Submit` channel 会死锁：外层任务占着槽位，满员时内层任务排不上。池子不为此加槽位，也不代为取消。

### Submit 立刻返回 channel，任务协程结束后送出一次 Result

`Submit` 是具体方法，类型参数写在方法上。Go 1.27 起具体方法可以声明自己的类型参数；接口方法仍然不行。见 [Go 博客：Generic Methods](https://go.dev/blog/generic-methods) 。因此一个非泛型的 `Pool` 可以提交不同的 `R`，但这个方法不能用来实现接口。

```go
func (p *Pool) Submit[R any](sign string, fn func() (R, error), opts ...Option) <-chan Result[R]
```

`sign` 是这次提交的来源标记，原样保存，不参与优先级和 ID 排序。空字符串允许。运行中的任务在 `TaskInfo.Sign` 里能看到它，告警在 `Alert.Sign` 里能看到它。

返回值是 `R` 能从 `fn` 推断出来的 channel，调用方通常不必写出 `[R]`。

```go
ch := p.Submit("user-api", func() (User, error) {
    return fetchUser(id)
})
got := <-ch
if got.Err != nil {
    return got.Err
}
// got.Value 的类型是 User
// got.Snapshot 是任务终态那一刻的池子，不是这次接收的那一刻
```

未传优先级时，优先级是 0。更高的 `int` 先执行，负数低于默认优先级。多个 `WithPriority` 同时出现时，最后一个生效。`opts` 里的 nil 项忽略。不限制优先级的取值范围。

```go
ch := p.Submit("report", func() (Report, error) {
    return buildReport()
}, loom.WithPriority(10))
```

函数签名固定为 `func() (R, error)`。池子不接收、不创建、不取消 `context`。函数若要协作取消，自己捕获 `context`；那只影响函数内部。池子看不到这次取消，不会把任务移出队列，也不会让 channel 提前收到错误。

业务错误由函数自己返回，放进 `Result.Err`，不包装。正常返回时 `R` 放进 `Result.Value`。

```go
type Result[R any] struct {
    Value    R
    Snapshot Snapshot
    Err      error
}
```

`Submit` 不返回 nil channel，也不另给一个 `error`。成功、业务错误、panic 和拒绝，都是 channel 里的那一次 `Result`。

channel 的容量是 1。进入终态并复制快照之后，任务协程离开协调者，发送这一次 `Result`，然后 `close`。发送发生在执行下一个用户函数之前，所以调用方不会被下一个任务的耗时拖住。容量为 1，没人接收时发送也不阻塞，槽位不会因此少一个。第二次接收得到零值 `Result` 和 `ok == false`，那不是一次任务结果。

调用方的 goroutine 不被 `Submit` 占住。接受操作先进容量为 100 的缓冲；缓冲满时，多出来的提交各用一条协程阻塞发送，调用方已经返回。协调者串行取出这些操作，有空闲槽位就启动任务协程，没有就进入优先级队列。不存在「调用方看到空闲槽位就自己跑函数」或「新任务抢走已经排着队的位置」的快路径。

接受发生在协调者里：

1. 分配任务 ID，`Submitted` 加 1。
2. 有空闲槽位时，这个任务直接标成 `Running`，协调者接着启动它的任务协程；没有时，任务进入优先级队列。有空闲槽位时队列一定是空的，所以直接标成 `Running` 不会插队。

任务 ID 是从 1 起、每次加 1 的 `uint64`，不复用。被拒绝的提交不占 ID。同优先级按 ID 从小到大执行，不使用时间戳。ID 就是提交顺序。

函数 panic 时，任务协程恢复现场。`Result.Value` 是零值，`Result.Err` 是 `*PanicError`。`errors.Is(err, ErrPanic)` 为真，`errors.As` 可取出 panic 的原始值和栈。栈是 `recover` 处 `debug.Stack` 的结果，属于任务协程，不是接收 channel 的那条 goroutine。错误文本不构成兼容承诺。协调者随后仍会按优先级启动下一个任务。

```go
type PanicError struct {
    Value any
    Stack []byte
}

func (e *PanicError) Error() string
func (e *PanicError) Unwrap() error // 返回 ErrPanic
```

终态只有两种：函数返回，或 panic 被恢复。进入终态的任务计入 `Completed`。被拒绝的提交没有任务，不计 `Submitted`。没有「排队时取消」这条终态。

```go
type Snapshot struct {
    Size         int
    Idle         int
    Running      int
    Waiting      int
    WaitingBy    []PriorityCount // 只含数量大于 0 的优先级，从高到低
    RunningTasks []TaskInfo      // 按任务 ID 从小到大，长度不超过 Size
    Submitted    uint64          // 累计接受数，不是当前在途数
    Completed    uint64          // 累计终态数，含成功、失败和 panic
    Failed       uint64          // 其中函数返回了非 nil 错误，或 panic
    Alerted      uint64          // 实际发出的告警次数，不是跨过的周期数
}

type PriorityCount struct {
    Priority int
    Count    int
}

type TaskInfo struct {
    ID         uint64
    Sign       string
    Priority   int
    WaitingFor time.Duration // 从接受到标成 Running，之后不再增加
    RunningFor time.Duration // 从标成 Running 到采集快照，是观察值
    Alerted    bool          // 这个任务至少发出过一次告警
}
```

`RunningTasks` 只包含已标成 `Running`、尚未终态的任务。等待队列可能很长，送出结果时不复制整份队列。等待侧只有 `Waiting` 和 `WaitingBy`。`WaitingBy` 和 `RunningTasks` 在协调者里新分配，调用方可以一直持有。时长用 `time.Since` 一类单调钟计算，不用墙钟相减。

快照和 `Alert` 都在不变量已经成立的那次协调者操作里复制，然后操作才返回。快照描述的是复制那一刻。调用方可以很晚才接收，期间调度还会变，这份快照不更新。它也不承诺之后还有空位。

协调者这次操作返回时下列式子成立：

- `Waiting > 0` 时 `Idle == 0`。有人排队就没有空闲槽位。
- `Idle + Running == Size`。
- `Running + Waiting == Submitted - Completed`。

因此，一个任务结束时，同一次协调者操作里先把它从运行表删除并计入终态，再按优先级把下一个排队任务标成 `Running`，然后才复制快照。队列非空时，这份快照不会把刚空出的槽位算成空闲；刚结束的任务自己不在 `RunningTasks` 中，下一个任务已经在里面。队列为空时，这个槽位还在，快照里算作 `Idle`。快照复制之后，任务协程先送出 `Result`，协调者才启动下一个用户函数。

`Idle > 0` 只说明采集时没有排队任务。随后的 `Submit` 不需要等别的任务结束，但仍然由协调者启动的任务协程执行。

```go
func (p *Pool) execute(job *task) {
    job.exec()             // 用户函数在协调者之外执行；panic 恢复成 *PanicError
    snap := p.finish(job)  // 回到协调者：终态、把下一个标成 Running、复制快照
    job.deliver(snap)      // 离开协调者后发送一次并 close；容量 1，不阻塞
    p.release()            // 再回到协调者，这时才启动下一个任务协程
}
```

### 拒绝也走 channel，能看见池子时就带快照

`p == nil`、`fn == nil` 在进入协调者之前拒绝。`p == nil` 优先于 `fn == nil`。两者都返回已经放好一次 `Result` 的 channel：`Err` 是对应哨兵，`Value` 和 `Snapshot` 都是零值，不读取池子，计数器不变。`Submit` 调用本身仍然立刻返回。

```go
var (
    ErrInvalidSize = errors.New("loom: invalid size")
    ErrNilPool     = errors.New("loom: nil pool")
    ErrNilFunc     = errors.New("loom: nil func")
    ErrPanic       = errors.New("loom: task panic")
)
```

哨兵用 `errors.Is` 比较。错误文本不构成兼容承诺。

计数器只统计已接受的任务：

| 事件 | Submitted | Completed | Failed |
| --- | --- | --- | --- |
| `p == nil`、`fn == nil` | 不变 | 不变 | 不变 |
| 接受，并标成 `Running` 或入队 | +1 | 不变 | 不变 |
| 函数返回 `(v, nil)` | 不变 | +1 | 不变 |
| 函数返回非 nil 错误，或 panic | 不变 | +1 | +1 |

### 池满时按优先级等待，已经开始的任务不会被换下

槽位都在执行时，新任务进入优先级队列。`Submit` 仍然立刻返回。调用方要等结果，就在自己的 goroutine 上接收 channel。队列没有容量上限，也没有取消入口。不为排队任务再开 goroutine。

取出规则：

- 数值更大的优先级先被标成 `Running`。
- 优先级相同，ID 更小的先被标成 `Running`。
- 已经标成 `Running` 的任务会执行到函数返回或 panic。新的高优先级任务不能把它换下来。

已经接受的任务一定会执行。函数自己提前 return，是唯一不跑完函数体的办法。

### 占用超时只在巡检仍看到任务执行时告警

每个 `Running` 任务记录标成 `Running` 的时刻。巡检 goroutine 只在告警开启时存在。它让协调者查看这些时刻，复制告警内容并更新计数，操作返回后再串行调用 `OnAlert`。

设阈值为 `T`，任务已发出的周期数为 `k`（初始 0），本次看到它仍在执行且 `elapsed = time.Since(start)`，`m = elapsed / T`（Go 的 `time.Duration` 相除，结果向零截断）。`m > k` 时发一次告警，然后把 `k` 设为 `m`。到期点是 `T`、`2T`、`3T`，以此类推。一次巡检跨过多个到期点也只发一次，`Alerted` 只加 1，不把错过的周期补成一串。`elapsed < T` 时不告警。

任务在下一次巡检看到它之前已经终态的，不补告警。因此「运行时间曾经超过阈值」不等于「一定会收到回调」。

巡检间隔是 `min(T, time.Second)`。这只约束巡检自己醒来的间隔：`T >= 1s` 时，决策时刻最多比到期点晚大约 1s；`T` 更短时，最多晚大约一个 `T`。`OnAlert` 在协调者之外按任务 ID 从小到大串行调用，回调花的时间会继续推迟后面的回调和下一次巡检。这个推迟不计入上面的间隔上界。

`At` 和 `RunningFor` 取协调者决策那次操作里的值，不包含排在前面的回调耗时。`Running` 包含正在告警的这条任务。`Idle`、`Running`、`Waiting` 满足前面的不变量。回调真正执行时，任务可能已经结束；`Alert` 描述的是决策时刻。

```go
type Alert struct {
    TaskID     uint64
    Sign       string
    Priority   int
    RunningFor time.Duration
    Idle       int
    Running    int
    Waiting    int
    At         time.Time
}
```

同一轮里先在协调者里为所有到期任务更新 `k`、池的 `Alerted` 计数和 `TaskInfo.Alerted`，操作返回后再回调。因此回调执行期间，别的 `Result` 已经能看到这次告警。`OnAlert` 为 nil 时同样更新，只是跳过调用。回调 panic 由巡检恢复，巡检继续处理本轮剩余任务和后续巡检；这次告警已经计过数，不撤回。

告警和调度是两条路。`OnAlert` 不取消任务，不调整优先级，不把任务协程从用户函数里拽出来。调用方若只看自己的 `Result`，可以读 `Snapshot.RunningTasks[].Alerted` 和 `Snapshot.Alerted`。那是终态那一刻的记录；任务还在跑的时候，通知靠 `OnAlert`。

池未满时同样巡检。一个任务卡住时，即使还没有人在排队，占用事实已经成立。巡检不退出。

## Rationale / 理由与取舍

### 并发上限由协调者持有的槽位实现，调用方自己决定要不要等

我们放弃用带缓冲 channel 当信号量、让调用方自己执行函数。那样做也能把同时执行的函数数限制住，但被占用的是调用方的 goroutine，池子没有一份可巡检的执行者名单，快照只能数信号量剩余槽位。channel 的等待顺序是 FIFO，塞不进「数值更大的优先级先执行，同优先级按任务 ID」。

我们也放弃用互斥锁保护调度状态。协调者协程串行执行操作，运行表和计数没有第二份写入者，就不需要锁。任务要执行时由协调者启动协程；执行完再回到协调者取当前运行情况。

协调者把「谁在执行」收成最多 `Size` 条记录。占用告警扫的就是这张表。

`Submit` 若阻塞到终态，每个排队调用都占住一条调用方 goroutine。返回容量为 1 的 channel 之后，等待变成调用方的选择：不接收就不占 goroutine，接收才等。任务协程的发送不能再阻塞，否则不接收的调用方会偷走一个并发名额。

用户函数里同步接收同一个池的 channel 时，当前任务占着槽位；没有空闲槽位时内层任务排不上，形成永久等待。这和信号量嵌套占用是同一类用法错误。池子不偷偷扩容，因为扩容会直接破坏并发上限。只调用 `Submit`、不接收，不会形成这个等待。

### 满员后等待，并且只排列尚未开始的任务

需求是池满时等待其他任务执行完成，再按优先级往下执行。我们按字面做成非抢占队列。

我们放弃抢占正在执行的任务。Go 不能安全地从外部停掉一个正在跑用户函数的 goroutine。函数没有 `context` 参数，池子也没有可以取消的句柄。把「高优先级到来就打断低优先级」写进池子，会让每个任务的耗时和错误语义都依赖后来者。

我们同样放弃池满立刻返回错误。那会把「等待」推回调用方，优先级队列也不再有唯一的实现位置。池满时 `Submit` 仍然立刻返回，等的是 channel，不是这次调用。

我们放弃「有空闲槽位时调用方直接执行」以及「结束任务后先把槽位算成空闲、离开协调者、再去领下一个」。那个窗口里队列可以非空而 `Idle > 0`，新的 `Submit` 会抢走本该给排队任务的槽位。所以用户函数只跑在协调者启动的任务协程上，分派和快照复制放在同一次协调者操作里，操作返回时 `Waiting > 0` 必然 `Idle == 0`。下一个用户函数要等本次 `Result` 送出后才启动，因此同时执行的用户函数不超过 `Size`。代价是调用方看不到「刚空出来、但马上又被队列占上」的那个瞬态。

### 告警用回调，在越过阈值时发出，不把通知推迟到 Result 被接收

只在 `Snapshot` 里做标记的话，堵住后面任务的那次慢调用自己还没终态，后来者也还拿不到它的 `Result`。告警要在占用已经发生的时候发出，所以用 `OnAlert`。

我们放弃告警后自动取消任务。需求写的是告警。自动取消会把巡检变成控制流，慢但正确的任务会被池子判失败。这一版的函数也没有可供池子取消的 `context`。

重复告警的相位从标成 `Running` 起算，间隔就是阈值本身，不再加一套重复间隔配置。只告警一次会让一个卡了很久的任务在第一次通知之后从运维视角消失。每个巡检周期都告警会在阈值很短时刷屏。巡检落后时把错过的周期补发成一串，同样会刷屏，所以多个到期点合并成一次，并把 `k` 拨到已经跨过的周期。

我们也不把「运行时长曾经超过阈值」承诺成「回调一定发生」。巡检看到的是任务仍在执行。在到期点和下一跳之间结束的任务不再补报。间隔上界只覆盖巡检自己的睡眠；慢回调的推迟单独存在，不写进同一个「最多晚 1s」。

### 等待队列不设上限，调用方自己少提交

我们放弃给队列一个默认容量并在超出时失败。容量选多少都是在替调用方做策略，选小了会把「池满则等待」变成「池满则失败」，选大了和无限队列没有实质差别。

我们也放弃用 `context` 取消排队任务。`Submit` 的签名里没有 `context`，函数也不接收它。已经接受的任务一定会跑完。

代价是调用方如果提交得比消费快，排队任务会一直占内存，而且没有任何 API 能把它们拿下来或停掉池子。每个排队任务持有闭包和一条容量为 1 的 channel，不持有调用方 goroutine。协调者和巡检一直留到进程结束。

### 低优先级可能一直排不到，第一版不做老化

持续到达的高优先级任务会让优先级更低的任务停在队列里。我们放弃提交越久优先级越高的老化。老化让「优先级数值」不再是稳定的顺序键，测试要覆盖时间，调用方也难以解释为什么一个低优先级任务插到了高优先级前面。

第一版的顺序键只有两个：优先级数值，以及同数值下的任务 ID。饿死是使用方式的问题，调用方可以减少高优先级流量，或给低优先级任务单独建池。

### 快照记录全部运行中任务，等待侧只保留计数

我们放弃在每次送出结果时复制整个等待队列。运行中任务数有硬上限 `Size`，复制它们的成本是稳定的，而「谁长期占用」问的就是这些任务。等待侧用总数和分优先级计数回答「前面还有多少」。`WaitingBy` 只保留数量大于 0 的优先级；它仍随不同优先级的个数增长，这是调用方选择了多少个优先级的结果，不是整份队列的副本。

快照钉在终态那次协调者操作，而不是钉在 `<-ch`。接收可以发生在很多个任务之后，若那时再拍一张，调用方就分不清自己这次任务结束时池子是什么样。

计数器只区分「没被接受」和「接受后跑完」。前者不是任务。后者计入 `Submitted` 和 `Completed`；函数失败或 panic 再计入 `Failed`。这样 `Submitted - Completed` 就是还在 `Running` 或 `Waiting` 里的数量。

### 不引入第三方协程池

现成池子的提交口通常是 `func()`，返回值要调用方自己塞 channel，优先级和占用告警也要在外面再包一层。那一层包完，和自己写一个小池子的代码量接近，还多一个核心依赖。这个池子的调度、告警和快照都在标准库能表达的范围内：`container/heap`、channel、`time`、`runtime/debug`。方法上的类型参数使用 Go 1.27 的语言能力，不是第三方包。

## Compatibility / 兼容性

仓库还没有池子的调用方，这次设计是纯增量。实现要求 Go 1.27 或更新，因为 `Submit` 是带类型参数的具体方法。接口方法不能声明类型参数，不能把 `Submit` 写进接口。

之后若给 `Snapshot`、`Alert`、`Result` 或 `PanicError` 增加字段，按字段名写的复合字面量仍能编译。不要把字段顺序当成承诺。`Submit` 的形状 `(sign string, fn func() (R, error), opts ...Option) <-chan Result[R]` 视为稳定。`TaskInfo.Sign` 和 `Alert.Sign` 是提交时的来源标记。`Result` 的字段名 `Value`、`Snapshot`、`Err` 视为稳定。channel 恰好送出一次然后关闭，视为稳定。优先级的排序规则视为稳定：数值更大的先执行，同数值按任务 ID 从小到大。错误身份以哨兵为准，不以错误字符串为准。panic 的原始值以 `PanicError.Value` 为准。

使用上的代价需要直接接受：

- `Submit` 不占调用方 goroutine。接受操作先进入容量为 100 的缓冲；超出的每个提交另有一条协程堵在发送上，协调者收下后那条协程就退出。1000 个排队任务仍是 1000 个闭包和 1000 条容量为 1 的 channel，外加 1 条协调者，以及最多 `Size` 条正在执行的任务协程；告警开启时再加 1 条巡检。接收 channel 的 goroutine 由调用方自己决定。
- 队列不封顶，也不能按任务取消，池子也不能关闭。内存随已接受、尚未执行的任务增长。协调者和巡检一直留到进程结束。
- 用户函数里同步接收同一个池的 channel，满员时会死锁。
- 运行时间超过阈值，但在巡检看到之前就结束的任务，不会有 `OnAlert`。
- 快照里的 `Idle` 不承诺下一次 `Submit` 还能立刻开始，也不描述接收时刻的池子。它只描述任务终态那一刻，并且那一刻没有排队任务。

没有旧版本需要迁移。

## Implementation / Transition / 实现与过渡

设计确认为 Accepted 之后再写代码。落地顺序：

1. `New`、协调者、泛型方法 `Submit`、容量为 1 的一次发送、`PanicError`、计数器，以及「终态后在同一次协调者操作里分派下一个，再复制快照，离开协调者后发送」。
2. 优先级堆。只从队首取，不按任务删除。
3. 巡检 goroutine、按周期合并的 `OnAlert`、回调 panic 恢复、`Alerted` 计数。

验收用 `go test`，至少覆盖这些路径：

- `Submit` 在函数返回之前就把 channel 交出来。队列为空时，接收到的 `Value` 是函数的 `R`，该任务不在 `RunningTasks` 中。`Idle + Running == Size`，且 `Idle >= 1`。channel 随后关闭，第二次接收的 `ok` 为 false。
- 调用方不接收时，任务协程仍能执行下一个任务。
- `Size` 为 1 时，先占住唯一槽位，再按优先级 1、10、5 提交三个任务。占住的任务放开之后，三个任务的开始顺序是 10、5、1。占住的任务的 `Result` 里，优先级 10 已经在 `RunningTasks` 中，`Idle == 0`，`Waiting == 2`。
- 任务执行超过 `OccupyThreshold` 且仍被巡检看到时，`OnAlert` 被调用，该任务的 `Result` 仍是原来的成功结果。一次巡检跨过多个周期只回调一次，`Alerted` 只加 1。在下一次巡检前结束的任务可以不告警。
- `OnAlert` panic 后，巡检还能对后续到期任务告警。
- 函数返回非 nil 错误时，`Result.Err` 就是该错误，`Failed` 加 1。
- 函数 panic 后，`errors.Is` 识别 `ErrPanic`，`errors.As` 得到原来的值和栈，池子还能执行下一个任务。
- `p == nil`、`fn == nil` 时，channel 在 `Submit` 返回前就已经放好对应哨兵和零值快照，不改变已有池的计数器。两者都为 nil 时是 `ErrNilPool`。

这一版没有独立的迁移工具，也没有特性开关。未确认前，业务代码不应依赖这些名字。

## Appendix / 附录

### 调用边界

| 条件 | 结果 |
| --- | --- |
| `Size <= 0` | `New` 返回 `nil, ErrInvalidSize`，没有 goroutine |
| `OccupyThreshold == 0` | 按 30s 告警 |
| `OccupyThreshold < 0` | 不启动巡检、不告警 |
| `p == nil` | 已放好的 channel，`ErrNilPool`，零值快照，不进协调者 |
| `fn == nil` 且池非 nil | 已放好的 channel，`ErrNilFunc`，零值快照，不进协调者 |
| `opts` 中的 nil、重复的 `WithPriority` | nil 忽略；优先级以后一个为准 |
| 已接受 | `Submit` 立刻返回；终态后发送一次并 `close` |
| 函数返回 `(v, err)` | `Result` 为 `{Value: v, Snapshot: snap, Err: err}`，`err` 不包装 |
| 函数返回非 nil 错误 | `Failed` 加 1 |
| 函数 panic | 零值、快照、`*PanicError`；`errors.Is` 为 `ErrPanic` |
| 没人接收 | 任务协程不阻塞，继续下一个任务 |
| 用户函数里同步接收同一个池的 channel | 满员时会死锁；不扩容 |

### 为什么 Submit 是方法

```go
func (p *Pool) Submit[R any](sign string, fn func() (R, error), opts ...Option) <-chan Result[R]
```

Go 1.27 允许具体方法声明类型参数，所以 `R` 不必再抬到包级函数上，也不必把池子做成 `Pool[R]`。做成 `Pool[R]` 会让一个池子只能执行一种返回类型。接口方法仍然不能声明类型参数，这个 `Submit` 不能出现在接口里。依据是 Go 1.27 的语言说明和 [Generic Methods](https://go.dev/blog/generic-methods) （2026-08-26）。

### 快照里的时长是观察值

`RunningFor` 在采集快照或决定告警时用 `time.Since` 计算，不是任务结束后的账单，也不是 `<-ch` 那一刻的时长。同一个任务在告警和在稍后的 `Result` 里，时长不同。`WaitingFor` 在标成 `Running` 时冻结，是从接受到开始执行的排队时间；之后不再增加。没进过队列、直接标成 `Running` 的任务，这个值只覆盖协调者分派本身的时间，不会被另记成 0。

### 告警相位

`k` 从 0 开始。巡检仍看到任务在执行，且 `elapsed/T > k` 时发一次，然后 `k = elapsed/T`。任务终态后 `k` 不再增加，也不补发。`Alerted` 按发出的次数加 1，不按 `k` 的跳变量加。
