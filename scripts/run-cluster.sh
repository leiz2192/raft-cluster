#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# 首次部署：在 node1 上引导一次（仅当数据目录为空）
if [ ! -d data/node1 ]; then
  echo "bootstrapping cluster on node1..."
  go run ./cmd/raft-meta init -config configs/node1.yaml
fi

# 三个节点并行启动
for i in 1 2 3; do
  go run ./cmd/raft-meta start -config configs/node$i.yaml &
done

wait
