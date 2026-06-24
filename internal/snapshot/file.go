package snapshot

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

func newFileStore(cfg config.SnapshotConfig, dataDir string, logger hclog.Logger) (raft.SnapshotStore, error) {
	retain := cfg.Retain
	if retain <= 0 {
		retain = 3
	}
	// hashicorp/raft 的 NewFileSnapshotStore(base) 总是在 base 下创建 "snapshots/"
	// 子目录（filepath.Join(base, "snapshots")），快照最终落 <base>/snapshots/。
	// base 用 cfg.Path；空则回退 dataDir，避免配置写 "xxx/snapshots" 又被 raft 再
	// append 一层 "snapshots" 形成 snapshots/snapshots。
	base := cfg.Path
	if base == "" {
		base = dataDir
	}
	if base == "" {
		return nil, fmt.Errorf("file snapshot store requires snapshot.path or dataDir")
	}
	// raft v1.7.1's NewFileSnapshotStore takes an io.Writer logOutput.
	// StandardWriter returns an io.Writer bridging hclog; StandardLogger
	// returns *log.Logger which does not satisfy io.Writer.
	return raft.NewFileSnapshotStore(base, retain, logger.StandardWriter(&hclog.StandardLoggerOptions{
		InferLevels: false,
	}))
}
