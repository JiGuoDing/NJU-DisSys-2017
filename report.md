# ***RAFT*** 实验报告

## 1-概述

本实验实现了 `RAFT` `consensus algorithm` 的主体部分。主要又分为两个部分：

1. **Leader Election**
2. **Log Replication**

## 2-分析与设计

- `Raft` 结构体

```go
type Raft struct {
 mu sync.Mutex

 peers []*labrpc.ClientEnd

 persister *Persister

 me int

 dead bool // 指示Raft是否被Kill()

 applyCh chan ApplyMsg

 RaftState // 将RaftState嵌入Raft中
}
```

- `RaftState` 结构体，存储 `Raft` 的状态信息

```go
type RaftState struct {
 CurrentTerm int

 VotedFor int

 Logs []LogEntry

 Role int

 VoteCnt int

 TimeStamp time.Time // 用于计时

 CommitIndex int

 LastApplied int

 NextIndex []int

 MatchIndex []int
}
```

- **常量定义**

```go
const (
 // raft实例的身份
 leader    = 0
 candidate = 1
 follower  = 2

 // ElectionTimeout上下限（ms）
 MinElectionTimeout = 150
 MaxElectionTimeout = 300

 // 心跳（AppendEntries RPC）发送周期(ms)
 HeartBeatInterval = 75
)
```

- **计时方式**

> 没有使用 `time.Timer` 计时器来进行计时，而是采用循环检测的方式，依靠在 `Raft` 结构体中额外定义了一个 `time.Time` 类型的时间戳 `TimeStamp` 实现，根据现在时间与时间戳之间的 **时间间隔** 判断是否达到 `Election Timeout` 和 `HeartBeatTimeout`。在一个*goroutine*执行的 `ticker()` 函数中，首先设置一个 `ElectionTimeout`， 接着进入 `for` 循环，计算前述的 **时间间隔**，根据当前 `raft` 实例的身份进入不同的代码块。进入代码块后首先要进行时间长度比较，如果 **时间间隔** 大于所设置的各种时延，那么就进入相应的执行区域，否则进入下一次循环。

### ***2.1 Leader Election***

2.1.1 发送投票请求

> 当 `raft` 实例的身份是*follower*或*candidate*时，如果 **时间间隔** 大于设置的 `ElectionTimeout`，该 `raft` 实例进入选举阶段。开一个*goroutine*进行选举，并重置该 `raft` 实例的时间戳为当前时间，同时重新设置一个 `ElectionTimeout`。
>
> 进入选举过程，首先将该 `raft` 实例的身份设置为*candidate*，并且为自己投一票。接着构造投票请求参数 `reqArgs`，为集群中的每一个 `peer` 开一个协程进行投票请求。

2.1.2 接收投票请求并处理

> 请求接收者首先判断该*candidate*是否来自过时任期，如果是则拒绝该投票请求。否则再判断该*candidate*的日志是否 **up-to-date**，先比较该*candidate*的最后一条日志所属的任期与自己的最后一条日志所属的任期，如果前者小于后者，则说明该*candidate*的日志过时，拒绝为其投票；如果前者等于后者，则再比较该*candidate*的最后一条日志的索引号与自己的日志长度-1的大小，如果前者小于后者，则说明该*candidate*的日志过时，拒绝为其投票。否则为该*candidate*投票。

2.1.3 处理投票结果
> 如果请求的目标没有给自己投票，分为两种情况：
>
> - 目标的任期大于自己的任期，说明存在更高任期的*leader*，等待该*leader*发送心跳来进行任期更新。
> - 目标的任期不大于自己的任期，说明自己的日志不 **up-to-date**，降级为*follower*并加入该任期，等待该任期的*leader*发送心跳来进行日志更新。
>
> 如果请求的目标投票给自己，那么自己的票数+1，判断获得的票数是否大于集群服务器个数的一半，若大于则成为当前任期的*leader*。

### ***2.2 Log Replication***


## 3-实验结果

### 测试结果图
![测试结果](./results/result.png)

## n-问题记录

- 一个follower断连再重连后，需要一个重新进入Term的过程，在此过程中，leader可能会将其NextIndex[]的值减少至0甚至一下，故需要防止NextIndex[]小于等于0
