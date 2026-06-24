#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=./bin/raft-meta

# 编译二进制：缺失或任一 .go 源码比二进制新时重新构建。
if [ ! -x "$BIN" ] || [ -n "$(find . -name '*.go' -newer "$BIN" -print -quit 2>/dev/null)" ]; then
  echo "building $BIN..."
  go build -o "$BIN" ./cmd/raft-meta
fi

# 首次部署：在 node1 上引导一次（仅当数据目录为空）
if [ ! -d data/node1 ]; then
  echo "bootstrapping cluster on node1..."
  "$BIN" init -config configs/node1.yaml
fi

# 三个节点并行启动（用编译好的二进制；业务日志走 data/nodeN/raft-meta.log）
for i in 1 2 3; do
  "$BIN" start -config configs/node$i.yaml &
done

wait
