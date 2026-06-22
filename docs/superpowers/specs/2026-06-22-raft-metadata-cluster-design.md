# raft-meta：基于 Raft 的强一致元数据存储集群设计

**日期**：2026-06-22
**状态**：已通过设计评审，待编写实现计划
**一句话**：用 Go + hashicorp/raft 实现一个 1 主 2 从的三节点强一致元数据存储（层次化 KV），用于集群管理与元数据存储。

---

## 1. 背景与目标

### 1.1 应用定位

构建一个**强一致的元数据存储服务**（类似 etcd/consul 的精简定位），用于集群管理、元数据存储和管理。Raft 负责领导者选举与日志复制，保证元数据在三个节点间一致；上层状态机是层次化 KV。

### 1.2 "1 主 2 从"语义

Raft 在 3 节点中选举出 1 个 leader（主）+ 2 个 follower（从）：
- 主处理所有写请求，并将日志复制到 2 个从
- 从接受读请求（读本地 FSM，可能脏读）
- 主宕机时 2 个从中选出新主

3 节点容忍 1 个故障（多数派 = 2）。

### 1.3 关键技术选型

| 维度 | 选择 | 理由 |
|------|------|------|
| 语言 | Go | Raft 生态最成熟，协程模型适合网络 IO |
| Raft 实现 | hashicorp/raft | 成熟可靠，省去手写协议 |
| 数据模型 | 层次化 KV（带前缀） | 通用、覆盖大部分元数据场景 |
| 访问接口 | HTTP RESTful | 调试方便，curl 可测 |
| 部署形态 | 单机多端口（开发）/ 配置驱动（可扩展多机） | 开发调试友好 |
| 读策略 | 主写从读（从可脏读） | 用户确认接受脏读换取读扩展性 |
| FSM 存储 | 内存 map + BoltDB 快照 | hashicorp/raft 标准玩法，性能好、耐久够 |

### 1.4 成功标准

- 3 节点集群能自动选主、复制写、对外提供 KV 读写
- 1 节点故障，集群继续可用（2/3 多数派）
- 3 节点全故障后恢复 2 个，已提交数据零丢失、自动恢复服务
- snapshot 存储后端可插拔（file/boltdb/s3）
- 单节点数据损坏可从 leader 重建；丢失多数派可强制单节点恢复

---

## 2. 整体架构与节点拓扑

### 2.1 三节点拓扑（单机多端口）

| 节点 | Raft 地址 (TCP) | HTTP 地址 | 数据目录 |
|------|----------------|----------|----------|
| node1 | 127.0.0.1:7001 | 127.0.0.1:8001 | ./data/node1 |
| node2 | 127.0.0.1:7002 | 127.0.0.1:8002 | ./data/node2 |
| node3 | 127.0.0.1:7003 | 127.0.0.1:8003 | ./data/node3 |

三个节点是**对等**的同一个二进制 `raft-meta`，靠配置文件区分身份（节点 id、端口、数据目录、集群 peer 列表）。

### 2.2 分层架构（每节点进程内部）

```
┌─────────────────────────────────────────────┐
│  HTTP API 层   (RESTful: /kv, /cluster, ...) │
│      ↓ 写                       ↑ 读        │
├─────────────────────────────────────────────┤
│  Raft 服务层    (hashicorp/raft)            │
│   · Leader 选举 · 日志复制 · 快照           │
│      ↓ Apply(log)              ↑ FSM.Read   │
├─────────────────────────────────────────────┤
│  FSM 状态机    (内存 map + BoltDB 快照)      │
│      ↓ Snap/Restore                         │
├─────────────────────────────────────────────┤
│  持久化层      BoltDB(log/stable) + 快照存储 │
└─────────────────────────────────────────────┘
```

### 2.3 节点加入方式（综合方案）

**首次成型：联合引导 + 一次性 `init`**
- 一份配置列出全部 3 个节点（id/raft 地址）
- 首次部署在任一节点跑 `raft-meta init`：对该节点调 `raft.BootstrapCluster(3-voter 配置)`，仅一次
- 三个节点都跑 `raft-meta start`：
  - 任意 2 个上线 → 选主 → 对外服务（2/3 多数派，容错从第 1 秒开始）
  - 第 3 个随时上线 → 自动接收日志追赶
- 重启时数据目录已有状态，跳过引导直接恢复

**容灾替换：集群成员 API**
- `/cluster/join` → leader 调 `raft.AddVoter`（加入新节点）
- `/cluster/remove` → leader 调 `raft.RemoveServer`（移除死节点）
- 专门服务于容灾场景下替换永久丢失的节点；日常固定 3 节点不涉及

**为何不用纯动态加入（模式 B）**：固定 3 节点用联合引导最简单健壮，2 节点即 2/3 多数派；纯动态加入的 2 节点启动会经历"先 1 节点（1/1 无容错）再加第 2 个"的脆弱期。同时保留成员 API 以备容灾替换，取两者之长。

---

## 3. 组件与模块划分

```
cmd/raft-meta/        # 入口：init / start / reset / recover 子命令
internal/
  config/             # 配置加载（节点id、raft/http地址、数据目录、集群peer列表、快照后端配置）
  raftnode/           # Raft 封装：建raft实例、BootstrapCluster、Apply、AddVoter/RemoveServer、leader查询
  fsm/                # FSM 实现：Apply 写内存map、Snapshot/Restore
  store/              # 上层KV语义：Put/Get/Delete/List，区分"写走raft.Apply"与"读本地FSM"
  api/                # HTTP handlers：/kv/*、/cluster/*，路由+序列化
  transport/          # TCP raft transport 封装（生产）+ InmemTransport 工厂（单测）
  snapshot/           # 可插拔快照存储后端（file/boltdb/s3）
server/               # 组装：把以上模块拼成一个可启动的节点进程
```

### 3.1 模块职责与依赖

| 模块 | 做什么 | 依赖 |
|------|--------|------|
| `config` | 解析配置文件/flag，给节点身份 | 无 |
| `transport` | 提供 raft 用的网络层（TCP/Inmem） | config |
| `fsm` | 实现 `raft.FSM`：Apply 改内存、Snapshot 落盘、Restore 加载 | snapshot（经 sink） |
| `raftnode` | 封装 hashicorp/raft：建实例、引导、提案、成员变更、状态查询 | transport, fsm, snapshot |
| `store` | KV 语义层：写→`raftnode.Apply`；读→`fsm` 本地查询 | raftnode, fsm |
| `api` | HTTP 路由与序列化，校验后调 `store` 或 `raftnode` | store, raftnode |
| `snapshot` | 可插拔快照后端工厂，实现 `raft.SnapshotStore` | config |
| `server` | 装配各模块、生命周期管理（启动/优雅关闭） | 全部 |

### 3.2 关键设计点

1. **`fsm` 与 `store` 分离**：`fsm` 只管"应用日志到状态"（被 raft 调用，不可主动发起提案）；`store` 是外部调用面，写时序列化命令再 `Apply`，读时直接问 `fsm`。raft 回放路径与客户端路径不耦合。

2. **命令编码**：写入 raft 的日志是序列化的命令 `{op: put/delete, key, value}`，JSON 起步（简单可读），FSM.Apply 反序列化后执行。接口不变，后续可换 protobuf。

3. **读写路径区分**：
   - 写：任意节点收到 → 若自己是 leader 直接 `Apply`；若不是，返回 leader 地址让客户端重定向
   - 读：主节点读本地 FSM（强一致）；从节点读本地 FSM（接受脏读），`?consistent=true` 时改为走 leader

4. **`raftnode` 暴露的状态查询**：`IsLeader()`、`LeaderAddr()`、`State()`、`Stats()`，供 `api` 和 `server` 用。

### 3.3 snapshot 存储可插拔

hashicorp/raft 里 snapshot 有两层正交概念，分别可插拔：

**① 存储后端**（实现 `raft.SnapshotStore`，决定快照字节存哪）：
- `FileSnapshotStore`：本地文件系统（默认）
- `BoltSnapshotStore`：单个 BoltDB 文件
- `S3SnapshotStore`（未来）：对象存储

**② FSM 序列化格式**（`fsm` 决定内存 map ↔ 字节流编解码，JSON/Gob/protobuf，与后端无关）。

```
snapshot/
  store.go          # 工厂：NewStore(cfg) raft.SnapshotStore，按 cfg.Type 返回实现
  file.go           # FileSnapshotStore 适配
  boltdb.go         # BoltDB 后端实现
  s3.go             # （未来）S3 后端，预留接口
  sink.go           # 各后端共用的 Sink 抽象（实现 raft.SnapshotSink）
```

配置：
```yaml
snapshot:
  type: file          # file | boltdb | s3
  path: ./data/node1/snapshots
  # s3:                  # type=s3 时
  #   bucket: ...
  #   prefix: ...
```

工厂伪码：
```go
func NewStore(cfg Config) (raft.SnapshotStore, error) {
    switch cfg.Snapshot.Type {
    case "file":   return raft.NewFileSnapshotStore(cfg.Snapshot.Path, retain, log)
    case "boltdb": return boltdbstore.New(cfg.Snapshot.Path)
    case "s3":     return s3store.New(cfg.Snapshot.S3)
    default:       return nil, fmt.Errorf("unknown snapshot type %q", cfg.Snapshot.Type)
    }
}
```

**收益**：`fsm` 只和字节流打交道，加新后端 `fsm` 不改；加新序列化格式后端不改；`raftnode` 只依赖 `raft.SnapshotStore` 接口，测试可注入 `InmemSnapshotStore`。log store / stable store 将来可用同模式做可插拔（本次默认 BoltDB，预留）。

---

## 4. 数据流

### 4.1 写路径（Put/Delete）

```
客户端 → POST /kv/{key}  (任意节点)
   │
   ▼
api: 校验请求
   │
   ▼
store.Put(key,val) → 序列化命令 {op:PUT, key, val}
   │
   ▼
raftnode.Apply(cmd, timeout)   ← 只能 leader 执行
   │  不是 leader？
   ├─→ 返回 307 + Location: leader 的 HTTP 地址（客户端重定向）
   │
   ▼ (是 leader)
raft 内部：追加日志 → 复制到 2 个 follower → 多数派确认 → 提交
   │
   ▼ (提交后 raft 回调)
fsm.Apply(logEntry): 反序列化命令 → 改内存 map
   │
   ▼
raftnode.Apply 返回 → store → api → 200 OK
```

要点：
- Apply 超时（默认 5s）内未提交 → 返回错误，客户端可重试
- 命令幂等性由"日志只应用一次"保证
- leader 重定向用 307；先不做代理转发（YAGNI）

### 4.2 读路径（Get/List）

两种语义，由查询参数控制：
```
GET /kv/{key}                    → 本地读（可能脏读）
GET /kv/{key}?consistent=true    → 强一致读（走 leader）
```

**本地读**（默认，主从都能服务）：
```
api → store.Get(key) → fsm.Get(key) → 读内存 map → 返回
```
- leader 上本地读 = 强一致（leader 的 FSM 含所有已提交）
- follower 上本地读 = 可能脏读（follower 可能落后）—— 用户接受的语义

**强一致读**（`?consistent=true`）：
```
api 判断本节点是否 leader
  是 → 本地读（强一致）
  否 → 307 重定向到 leader
```
先用"重定向到 leader 读"实现强一致，简单正确。Raft 的 ReadIndex/Lease 读（follower 也强一致）是后续优化，本次不做。

### 4.3 快照路径（Snapshot）

触发：日志条数达阈值（默认 1024）或定时（默认每 10 分钟），raft 自动发起。
```
raft 触发 → fsm.Snapshot() 返回 FSMSnapshot
   │
   ▼
FSMSnapshot.Persist(sink):
   遍历内存 map → 序列化 → 写入 sink
   (sink 背后是 file/boltdb/s3，由可插拔后端决定)
   ▼
sink.Close() → 快照落盘/落存储
   │
   ▼
raft 截断旧日志（保留快照之后的）
```
要点：
- Snapshot 期间 FSM 仍可服务读写（`sync.RWMutex` 保护，`Persist` 拷贝一致视图再序列化，不阻塞 Apply）
- 快照保留数（retain）配置化，默认 3

### 4.4 恢复路径（Restore）

节点启动时：
```
raftnode 启动 → 加载持久化状态（BoltDB: log + stable）
   │
   ▼
raft 检测到最新 snapshot → fsm.Restore(reader)
   │
   ▼
fsm.Restore: 从 reader 读字节 → 反序列化 → 重建内存 map
   │
   ▼
raft 重放 snapshot 之后的日志 → fsm.Apply 逐条
   │
   ▼
FSM 状态恢复到故障前最后提交点 → 节点加入集群/参与选举
```

---

## 5. 错误处理与边界

### 5.1 写请求到非 leader
- `store` 调 `Apply` 前先查 `IsLeader()`；不是则返回 leader 的 HTTP 地址
- API 返回 `307 Temporary Redirect` + `Location: <leader-http-addr>`
- leader 未知（选举中）：返回 `503` + `Retry-After`
- 可选"代理转发"开关（默认关）

### 5.2 Apply 超时 / 提交失败
- Apply 超时（默认 5s）内未提交 → `ErrTimeout`，API 返回 `504`
- 超时不代表没提交，可能只是慢：客户端用**幂等写**（`idempotency-key`）重试，FSM.Apply 检测重复 key 跳过；去重表也走 raft
- 网络分区导致 leader 失效：Apply 持续失败直到新 leader 选出，客户端重试自然落到新 leader

### 5.3 节点崩溃与重启
- **单节点崩溃**：剩 2 节点继续服务（2/3 多数派）；崩溃节点重启后 Restore + 日志重放追赶
- **2 节点崩溃（剩 1）**：1/3 < 多数派，无法选主提交，集群**只读降级**（本地读可，写全失败），等第 2 个恢复
- **3 节点全崩、恢复 2 个**：2/3 多数派 → 自动恢复服务，已提交数据零丢失（走 4.4 恢复路径）
- **持久化失败**（写 BoltDB 失败）：节点主动 `raft.Shutdown()` 退出，避免脑裂；运维介入

### 5.4 脑裂与分区
- Raft 天然防脑裂：分区后只有多数派侧能选主提交，少数派侧无法提交写（Apply 超时失败）
- 旧 leader 分区：旧 leader 在少数派侧仍接受写但无法提交 → Apply 超时 → 客户端重试；多数派侧选新 leader 后旧 leader 回归发现 term 更高，自动降级 follower，未提交日志回滚

### 5.5 快照边界
- **快照期间写入**：FSM 用 `sync.RWMutex` 保护；`Persist` 拿读锁拷贝一致视图再序列化，不阻塞 Apply
- **快照损坏/读失败**：`Restore` 返回错误 → 节点拒绝启动并报错（不静默用空状态，否则丢数据）；保留多份快照（retain=3）可手工回退
- **快照与日志不一致**：raft 保证 snapshot.index 之后才有日志；若检测到冲突以 snapshot 为准重建

### 5.6 成员变更边界（容灾替换）
- `AddVoter`/`RemoveServer` 必须在 leader 上调用，非 leader 返回 307
- **移除死节点**：`RemoveServer` 后集群变 2 节点（多数派=2，需两都在线才提交）→ 提醒运维尽快 `AddVoter` 新节点恢复 3 节点容错
- **加入新节点**：新节点先以空状态启动，leader `AddVoter` 后通过日志复制追赶，追上后才算正常投票成员

### 5.7 配置与启动边界
- 数据目录已存在状态 + 用户误传 `init` → 报错拒绝（"cluster already bootstrapped"），防重复引导
- 数据目录为空 + 用户传 `start`（没 init 过）→ 报错提示先 `init`，或按配置尝试联系已有 leader 加入
- 配置非法（端口冲突、peer 列表不全 3 个）→ 启动前校验失败退出

### 5.8 资源与背压
- Apply 慢导致日志堆积：raft 配置 `SnapshotThreshold` 触发快照截断，避免日志无限增长
- 客户端并发写过大：HTTP 层加 `maxInflight` 限流（默认 256），超出返回 `429`

### 5.9 单节点数据损坏修复

3 节点里 1 个节点持久化数据（BoltDB log/stable + 快照）损坏，另 2 个健康 = 多数派，持有全部已提交数据。损坏节点从 leader 重建，**零丢失**。

**修复流程：擦除 + 重加入**
```
1. 停掉损坏节点
2. 清空该节点数据目录（log + stable + 快照，全部）
   → raft-meta reset --node node2 --yes 一键安全清空（带确认）
3. 重新 start 该节点
   - 数据目录空 → 不调 BootstrapCluster（集群已存在）
   - 用配置里的 peer 地址联系 leader
4. leader 检测到该 follower 落后太多 → 触发 InstallSnapshot
   - 推送完整快照 → 节点 Restore 重建内存 map
   - 再补快照之后的日志 → fsm.Apply 逐条
5. 追上后恢复为正常投票成员
```

不用 `AddVoter`：联合引导下该节点本来就在 voter 列表（raft ID 来自配置，不随数据目录变化），擦数据目录不改 ID，仍是合法 voter，leader 直接同步。

边界：
- 擦除必须彻底（log + stable + 快照三处全清），部分擦除留不一致状态更糟，`reset` 保证全清
- 损坏的是 leader 本身：leader 持久化失败自动 step down，2 个健康 follower 选新 leader，再擦除重加入降级后的损坏节点
- 检测信号：节点启动时 BoltDB 打不开 / 快照读失败 → 拒绝启动并报错（见 5.5），运维据此擦除重加入
- 2 个节点都损坏（只剩 1 健康）：1/3 < 多数派，无法靠重加入自愈 → 走 5.10 强制单节点恢复

### 5.10 丢失多数派的强制单节点恢复

3 节点里 2 个数据丢失，只剩 1 个节点的数据。这是最严重容灾场景，能恢复但**可能丢已提交数据**。

**核心认识**：只剩 1 节点 = 丢失多数派。Raft 的"已提交 = 多数派持久化"保证失效——孤立 1 节点不一定含全部已提交数据，单节点恢复是最后手段，必须接受可能丢已提交数据。

**修复机制：`raft.RecoverCluster` 强制恢复**
```
1. 停掉所有节点
2. 选数据最新的存活节点（lastIndex / 快照最新）—— 尽量减少丢失
3. 该节点上跑 raft-meta recover --force：
   - 直接打开它的 BoltDB log/stable + 快照存储（绕过 raft 运行时）
   - 调 raft.RecoverCluster(..., Configuration{仅本节点1个voter})
     · 加载最新快照 → Restore FSM
     · 重放日志到 commitIndex
     · 把配置强制改写为单节点（移除 2 个死节点）
4. 单独 start 该节点 → 1/1 多数派 → 自选 leader → 对外提供它所持有的已提交数据
5. 准备 2 个新节点（擦空），start，从恢复出的 leader AddVoter 加入追赶
6. 追齐 → 恢复 3 节点容错
```

为何必须强制改配置：不移除 2 个死节点，集群仍是 3-voter，单节点永远 1/3 无法选主；改写成 1-voter 后单节点即多数派能选主，人为"承认"那 2 个节点永久丢了。

代价与边界：
- **可能丢已提交数据**：存活 1 节点若是落后 follower，丢失更大；`RecoverCluster` 只能恢复该节点磁盘上有的数据
- 未提交日志丢弃（正确行为）
- **历史分叉**：恢复后之前从死节点读到过已提交数据的客户端可能看到数据"消失"，运维须对外通告
- `recover` 命令必须 `--force` + 交互式二次确认 + 醒目日志，绝不静默执行
- 有多于 1 个残存节点时选 lastIndex 最高的恢复，其余丢弃

**三种损坏场景对照**：

| 场景 | 多数派 | 修复方式 | 已提交数据 |
|------|--------|----------|-----------|
| 1 节点损坏，2 健康 | 2/3 在 | 擦除 + 重加入（leader 同步） | 零丢失 |
| 2 节点丢失，1 存活 | 丢失 | `RecoverCluster` 强制单节点恢复 + 重建 | 可能丢失 |
| 3 节点全丢 | 丢失 | 只能从外部快照备份恢复 | 取决于备份 |

**预防**（比修复重要）：
- 定期快照备份到外部存储（复用 3.3 S3 后端），3 节点全丢时唯一兜底
- 监控 2 节点降级，只剩 2 节点健康立刻告警修复，避免滑到"只剩 1 个"

---

## 6. 测试策略

### 6.1 单元测试（模块隔离）

| 模块 | 测什么 | 手段 |
|------|--------|------|
| `fsm` | Apply 正确改 map、Snapshot/Restore 往返一致、重复 Apply 幂等 | 直接构造 logEntry 调 Apply；Persist→Restore 对比 |
| `store` | Put/Get/Delete/List 语义、读路径分支 | mock `raftnode`，验证写调 Apply、读调 fsm |
| `snapshot` | 各后端 Create/List/Open/Persist 往返、工厂按 cfg 返回正确实现 | `InmemSink` + 真实 file/boltdb 后端各跑一遍 |
| `config` | 解析、默认值、非法配置报错 | 表驱动用例 |
| `api` | 路由、参数校验、307 重定向、状态码 | `httptest` + mock store/raftnode |

`fsm` 和 `snapshot` 的可插拔设计让它们能用 inmem 后端快速测，不依赖真实文件系统。

### 6.2 集成测试（真 raft，多节点）

用 `InmemTransport` + `InmemSnapshotStore` + 内存 log store 在单进程内拉起 3 节点集群（毫秒级启动），覆盖：
- **选主**：启动后唯一 leader；杀 leader 后新 leader 在选举超时内选出
- **写复制**：写 leader → 3 节点 FSM 状态一致
- **读语义**：follower 本地读可能脏、leader 本地读强一致、`?consistent=true` 重定向
- **快照**：触发 `SnapshotThreshold` 后快照生成、日志截断、状态不变
- **重启恢复**：Shutdown 一节点 → 重启 → Restore + 日志重放 → 状态追平
- **成员变更**：AddVoter 加入新节点、RemoveServer 移除节点

### 6.3 容灾场景测试（核心关切）

| 场景 | 预期 |
|------|------|
| 3 节点全 Shutdown，恢复 2 个 | 2/3 多数派选主，已提交数据零丢失，可读写 |
| 3 节点全 Shutdown，恢复 1 个 | 1/3 无法选主，写失败、本地读可降级服务 |
| 恢复 2 个后，第 3 个迟到上线 | 自动追赶，追上后恢复 3 节点容错 |
| 永久丢 1 个 + AddVoter 新节点 | 新节点加入追赶，RemoveServer 死节点，恢复 3 节点 |
| 1 节点数据损坏，擦除重加入 | leader InstallSnapshot 同步，状态追平，零丢失 |
| 2 节点丢失，`RecoverCluster` 单节点恢复 | 单节点自选主，持有数据可读；AddVoter 重建 3 节点 |
| 旧 leader 分区 | 少数派侧写超时失败，多数派选新主，旧主回归降级 |

用 InmemTransport 的网络分区模拟（关掉某节点 transport）实现，无需真实网络。

### 6.4 HTTP 端到端测试

起真实 3 进程集群（单机多端口，测试脚本拉起）：
- curl 写 leader、读 follower、验证复制
- 307 重定向链路、幂等重试（带 idempotency-key）
- 故意 kill leader 进程，验证客户端重试落到新 leader

### 6.5 性能与压力（基线，不硬性优化）
- 基线 benchmark：单 key 写 QPS、3 节点复制延迟、快照耗时
- 留 benchmark 用例，不设硬指标——元数据场景量级小，确认无异常退化即可

### 6.6 测试基础设施
- 集成测试用可调时序的 raft config（低 ElectionTimeout/Heartbeat，秒级完成选主）
- 测试 helper：`newTestCluster(n)` 一行拉起 n 节点 inmem 集群
- CI：`go test ./...` 跑全部单测+集成；HTTP e2e 单独 stage

---

## 7. 未做（YAGNI，明确排除）

- Raft 协议手写实现（用 hashicorp/raft）
- 代理转发写请求（先做 307 重定向）
- ReadIndex/Lease 强一致 follower 读（先重定向到 leader）
- 动态加减节点的完整编排（仅保留容灾替换用的成员 API）
- 日志/快照的 protobuf 序列化（先 JSON）
- 自动化快照备份/恢复编排（预留 S3 后端，编排不在首期）
- K8s/Docker 部署编排（先单机多端口）
- 硬性性能指标优化

---

## 8. 后续步骤

本 spec 通过用户审阅后，调用 `writing-plans` skill 编写实现计划。
