package raftnode

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
	"raft-meta/internal/snapshot"
)

// Reset wipes all persistent state in dataDir (log + stable + snapshots).
// 用于单节点数据损坏后从 leader 重建：擦除后重启即可。
func Reset(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dataDir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// RecoverClusterSingle forcibly rewrites the cluster config to a single voter
// (this node) and restores FSM from the latest snapshot + log replay. 用于
// 丢失多数派（只剩 1 节点）的最后手段恢复；可能丢已提交数据。
func RecoverClusterSingle(cfg *config.Config, logger hclog.Logger) error {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("mkdir dataDir: %w", err)
	}
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("bolt store: %w", err)
	}
	defer boltStore.Close()

	snaps, err := snapshot.NewStore(cfg.Snapshot, logger)
	if err != nil {
		return err
	}

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.Logger = logger

	// raft v1.7.1: NewInmemTransport returns (ServerAddress, *InmemTransport).
	// The transport is only used internally by RecoverCluster (e.g. to encode
	// the configuration into a snapshot); it is not persisted or returned.
	_, trans := raft.NewInmemTransport(raft.ServerAddress(cfg.RaftAddr))

	configuration := raft.Configuration{
		Servers: []raft.Server{{
			ID:      raft.ServerID(cfg.NodeID),
			Address: raft.ServerAddress(cfg.RaftAddr),
		}},
	}

	// raft.RecoverCluster refuses to run when there is no existing state
	// (empty log + no snapshots + currentTerm==0), as an operator-error guard.
	// For the DR "force single-node" path we explicitly intend to (re)initialize
	// this node as a single-node cluster, so seed currentTerm=1 to satisfy the
	// guard. This mirrors what raft.BootstrapCluster does on a fresh dir. On a
	// node that already has state, HasExistingState is already true and this
	// SetUint64 is a harmless no-op overwrite of an equal-or-smaller term.
	hasState, err := raft.HasExistingState(boltStore, boltStore, snaps)
	if err != nil {
		return fmt.Errorf("check existing state: %w", err)
	}
	if !hasState {
		if err := boltStore.SetUint64([]byte("CurrentTerm"), 1); err != nil {
			return fmt.Errorf("seed current term: %w", err)
		}
	}

	// RecoverCluster 加载最新快照 → Restore FSM → 重放日志到 commitIndex →
	// 把配置强制改写为单节点。
	if err := raft.RecoverCluster(raftCfg, fsm.New(), boltStore, boltStore, snaps, trans, configuration); err != nil {
		return fmt.Errorf("recover cluster: %w", err)
	}
	return nil
}
