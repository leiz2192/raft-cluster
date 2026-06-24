package snapshot

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

// NewStore returns a raft.SnapshotStore based on cfg.Type. dataDir is the
// fallback base directory for the file backend when cfg.Path is empty.
//
// 后端与 FSM 序列化格式正交：本工厂只决定快照字节存哪。
// 注意：hashicorp/raft 的 FileSnapshotStore 总是在 base 下再建一个 "snapshots/"
// 子目录，故最终快照落 <base>/snapshots/。base = cfg.Path（空则 dataDir）。
func NewStore(cfg config.SnapshotConfig, dataDir string, logger hclog.Logger) (raft.SnapshotStore, error) {
	switch cfg.Type {
	case "file":
		return newFileStore(cfg, dataDir, logger)
	case "inmem":
		return NewInmem(logger), nil
	default:
		return nil, fmt.Errorf("unsupported snapshot type %q (supported: file, inmem; s3 待实现)", cfg.Type)
	}
}
