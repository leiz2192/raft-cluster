# raft-meta

基于 hashicorp/raft 的 1 主 2 从三节点强一致元数据存储（层次化 KV）。

## 构建

    go build ./cmd/raft-meta

## 启动集群（单机多端口）

    ./scripts/run-cluster.sh

首次运行会在 node1 上引导一次（仅当 data/node1 不存在），然后并行启动 3 节点。

## 操作

    # 写（任意节点，非 leader 自动 307 重定向）
    curl -X PUT http://127.0.0.1:8001/kv/nodes/n1 -d '{"value":"up"}'
    # 读（默认本地读，可能脏读；?consistent=true 强一致走 leader）
    curl http://127.0.0.1:8002/kv/nodes/n1
    curl http://127.0.0.1:8002/kv/nodes/n1?consistent=true
    # 列表
    curl 'http://127.0.0.1:8001/kv?prefix=/nodes/'
    # 集群状态
    curl http://127.0.0.1:8001/cluster/status
    # 手动触发快照（绕过阈值，截断日志；低写入场景可由外部定时器周期调用）
    curl -X POST http://127.0.0.1:8001/cluster/snapshot

## 容灾

**单节点数据损坏**（另 2 个健康，零丢失）：

    raft-meta reset -config configs/node2.yaml   # 擦除损坏节点数据
    raft-meta start -config configs/node2.yaml   # 重启，leader 自动 InstallSnapshot 同步

**丢失多数派（只剩 1 节点，可能丢已提交数据）**：

    raft-meta recover -config configs/node1.yaml  # 强制单节点恢复
    raft-meta start  -config configs/node1.yaml   # 单节点自选主对外
    # 再 AddVoter 2 个新节点恢复 3 节点：
    curl -X POST http://127.0.0.1:8001/cluster/join -d '{"id":"node2","addr":"127.0.0.1:7002"}'

## 测试

    go test ./...

设计文档：docs/superpowers/specs/2026-06-22-raft-metadata-cluster-design.md
