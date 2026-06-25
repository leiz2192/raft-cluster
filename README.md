# raft-meta

基于 hashicorp/raft 的 1 主 2 从三节点强一致元数据存储（层次化 KV）。

## 构建

    go build ./cmd/raft-meta

## 启动集群（单机多端口）

    ./scripts/run-cluster.sh

首次运行会在 node1 上引导一次（仅当 data/node1 不存在），然后并行启动 3 节点。

## 配置

三节点**同端口、不同回环地址**区分（127.0.0.0/8 整段均为 loopback，可分别 bind）：

| 节点 | raft | 业务 HTTP | 调试 pprof | 数据目录 |
|------|------|----------|-----------|---------|
| node1 | 127.0.0.1:7000 | 127.0.0.1:8000 | 127.0.0.1:6060 | ./data/node1 |
| node2 | 127.0.0.2:7000 | 127.0.0.2:8000 | 127.0.0.2:6060 | ./data/node2 |
| node3 | 127.0.0.3:7000 | 127.0.0.3:8000 | 127.0.0.3:6060 | ./data/node3 |

`configs/nodeN.yaml` 关键字段：

- `raftAddr`/`httpAddr`/`dataDir`：raft/HTTP 监听地址与数据目录
- `peers`：3 节点 voter 列表（联合引导用，各节点回环地址 + 同一 raft 端口）
- `snapshot.type`：快照存储后端（`file` | `inmem`）。`file` 时快照落 `<snapshot.path>/snapshots/`（raft 总会再建一层 `snapshots/` 子目录）；`snapshot.path` 空 → 用 `dataDir`，即 `<dataDir>/snapshots/`
- `logStore.type`：raft log/stable store 后端（`inmem` | `boltdb` | `rocksdb`(预留)）；留空则按 `dataDir` 自动选（有 `dataDir`=`boltdb`，无=`inmem`）
- `raft`：raft 时序/阈值。`applyTimeout`（写 Apply 超时，Duration 如 `5s`，空=5s）、`snapshotInterval`（快照检测间隔，Duration 如 `10m`，空=10m）、`snapshotThreshold`（快照日志条数阈值，空=1024）
- `log`：业务日志。`file` 空 → 写 stderr（旧行为）；设了 `file` → 落文件并按 lumberjack 轮转。`maxSize` 支持 Size 写法（`100MB`/`1GB`，二进制 KB=1024，空=100MB）；`maxBackups` 份数（空=7）；`maxAge` 天（空=30）；gzip 压缩旧份。`json: true` → JSON，默认 `false`=text。`level` 空=info。raft 自身日志走同一 logger、同文件。
- `debug.addr`：pprof 调试端口（同端口、各节点回环地址）；空 → 不开

> Size/Duration：`maxSize` 等大小字段支持 `100MB`/`1GB`/`512KiB`（二进制，裸数字=字节）；`applyTimeout`/`snapshotInterval` 等时长支持 `5s`/`10m`/`1h`/`1d`/`7days`/`1d30m`（`time.ParseDuration` + 天，裸数字=秒）。

## 操作

    # 写（任意节点，非 leader 自动 307 重定向）
    curl -X PUT http://127.0.0.1:8000/kv/nodes/n1 -d '{"value":"up"}'
    # 读（默认本地读，可能脏读；?consistent=true 强一致走 leader）
    curl http://127.0.0.2:8000/kv/nodes/n1
    curl http://127.0.0.2:8000/kv/nodes/n1?consistent=true
    # 列表
    curl 'http://127.0.0.1:8000/kv?prefix=/nodes/'
    # 集群状态（本节点）
    curl http://127.0.0.1:8000/cluster/status
    # 集群全量状态（经本节点扇出查所有节点，?full=true）
    curl http://127.0.0.1:8000/cluster/status?full=true
    # 手动触发快照（绕过阈值，截断日志；低写入场景可由外部定时器周期调用）
    curl -X POST http://127.0.0.1:8000/cluster/snapshot

## 监控指标

`/metrics` 暴露 Prometheus 格式指标（Prometheus/Grafana 可直接抓取）。`/cluster/status` 也含 `is_leader`/`fsm_keys`/`peers`/`commit_index` 等扩展字段供人查看。

主要指标（前缀 `raft_meta_`）：

- 状态 gauge：`is_leader`、`raft_term`、`commit_index`、`applied_index`、`last_log_index`、`last_snapshot_index`、`fsm_keys`、`peers`
- KV 操作：`kv_ops_total{op}`、`kv_op_errors_total{op}`、`kv_apply_duration_seconds{op}`（put/delete）、`kv_read_duration_seconds{op}`（get/list）
- 快照：`snapshot_triggers_total`（手动触发次数）
- HTTP：`http_requests_total{method,code}`、`http_request_duration_seconds{method}`

    curl http://127.0.0.1:8000/metrics

## 调试（pprof，独立端口）

pprof 挂在**独立调试端口**（`debug.addr`，各节点 127.0.0.1/2/3:6060），与业务端口（:8000）隔离。`debug.addr` 空 → 不开 pprof。

    go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30   # CPU
    go tool pprof http://127.0.0.1:6060/debug/pprof/heap                  # 堆
    curl http://127.0.0.1:6060/debug/pprof/goroutine?debug=2              # goroutine 栈

业务端口（:8000）不响应 `/debug/pprof/`。

## 容灾

**单节点数据损坏**（另 2 个健康，零丢失）：

    raft-meta reset -config configs/node2.yaml   # 擦除损坏节点数据
    raft-meta start -config configs/node2.yaml   # 重启，leader 自动 InstallSnapshot 同步

**丢失多数派（只剩 1 节点，可能丢已提交数据）**：

    raft-meta recover -config configs/node1.yaml  # 强制单节点恢复
    raft-meta start  -config configs/node1.yaml   # 单节点自选主对外
    # 再 AddVoter 2 个新节点恢复 3 节点：
    curl -X POST http://127.0.0.1:8000/cluster/join -d '{"id":"node2","addr":"127.0.0.2:7000"}'

## 测试

    go test ./...

设计文档：docs/superpowers/specs/2026-06-22-raft-metadata-cluster-design.md
