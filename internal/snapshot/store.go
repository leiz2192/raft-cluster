package snapshot

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
)

// NewStore returns a raft.SnapshotStore based on cfg.Type.
// 后端与 FSM 序列化格式正交：本工厂只决定快照字节存哪。
func NewStore(cfg config.SnapshotConfig, logger hclog.Logger) (raft.SnapshotStore, error) {
	switch cfg.Type {
	case "file":
		return newFileStore(cfg, logger)
	case "inmem":
		return NewInmem(logger), nil
	default:
		return nil, fmt.Errorf("unsupported snapshot type %q (supported: file, inmem; s3 待实现)", cfg.Type)
	}
}
