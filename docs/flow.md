# 流程详情

总图在 [README.md](../README.md#流程) 。

## Submit

`Submit` 只做检查和投递。nil 结果在返回前就已经放进 channel，不进协调者。

```mermaid
flowchart TD
  A["Submit"] --> B{"p 为 nil?"}
  B -->|是| C["送出 ErrNilPool 并关闭"]
  B -->|否| D{"fn 为 nil?"}
  D -->|是| E["送出 ErrNilFunc 并关闭"]
  D -->|否| F["解析优先级，默认 0，nil 选项忽略"]
  F --> G{"容量 100 的缓冲未满?"}
  G -->|是| H["直接放入缓冲"]
  G -->|否| I["另开协程阻塞发送"]
  H --> J["调用方立即返回 channel"]
  I --> J
```

## 协调者

协调者每次只处理一个接受操作。任务 ID 在这里分配。降级成功不计 `Submitted`，也不进入运行表。

```mermaid
flowchart TD
  A["取出 accept"] --> B["分配任务 ID"]
  B --> C{"运行数小于 Size?"}
  C -->|是| D["Submitted 加 1，标成 Running"]
  D --> E["本轮操作结束后启动任务协程"]
  C -->|否| F{"Degrade 已设置?"}
  F -->|否| G["Submitted 加 1，按优先级入队"]
  F -->|是| H["把当前运行任务交给 Degrade"]
  H --> I{"返回 true 且未 panic?"}
  I -->|否| G
  I -->|是| J["池外执行：不占 Size，不计 Submitted"]
```

## 池内终态

池内任务结束后，先记终态并送出 `Result`，然后才启动下一个用户函数。

```mermaid
flowchart TD
  A["执行用户函数"] --> B{"panic?"}
  B -->|是| C["恢复为 PanicError"]
  B -->|否| D["记下 Value 和 Err"]
  C --> E["回到协调者 finish"]
  D --> E
  E --> F["移出运行表，Completed 加 1，有错误则 Failed 加 1"]
  F --> G{"队列还有任务?"}
  G -->|是| H["弹出最高优先级，标成 Running，放入 ready"]
  G -->|否| I["复制这一刻的快照"]
  H --> I
  I --> J["送出一次 Result 并关闭 channel"]
  J --> K{"ready 里有下一个?"}
  K -->|是| L["release 后启动它的用户函数"]
  K -->|否| M["这一轮结束"]
```

## 池外执行

池外任务不经过槽位交接。它只复制结束时的快照，再从原来的 channel 送出。

```mermaid
flowchart TD
  A["池外执行用户函数"] --> B["回到协调者，只复制当前快照"]
  B --> C["从原来的 channel 送出 Result 并关闭"]
```

## 巡检

巡检与任务并行。任务仍在运行，并且跨过新的占用周期时，才通知一次。

```mermaid
flowchart TD
  A{"OccupyThreshold"} -->|小于 0| B["不启动巡检"]
  A -->|等于 0| C["阈值按 30s"]
  A -->|大于 0| D["使用配置值"]
  C --> E["间隔取阈值和 1s 的较小值"]
  D --> E
  E --> F["每个周期让协调者采集运行中的任务"]
  F --> G{"仍在运行，且跨过一个或多个新周期?"}
  G -->|否| H["不告警；结束得太早的不补发"]
  G -->|是| I["这一次合并成一条告警，Alerted 加 1"]
  I --> J{"OnAlert 为 nil?"}
  J -->|是| K["只计数"]
  J -->|否| L["协调者外按任务 ID 顺序回调"]
  L --> M["回调 panic 被恢复，巡检继续"]
```
