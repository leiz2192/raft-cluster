package raftnode

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
	"raft-meta/internal/snapshot"
)

type Node struct {
	cfg      *config.Config
	raft     *raft.Raft
	fsm      *fsm.FSM
	trans    raft.Transport
	logs     raft.LogStore
	stable   raft.StableStore
	bolt     io.Closer // non-nil when persistence is BoltDB; closed on Shutdown
	snaps    raft.SnapshotStore
	logger   hclog.Logger
}

// New constructs a Node. Transport and persistence are decoupled:
//   - transport: inmem when cfg.UseInmemTransport, else TCP
//   - persistence: BoltDB (log+stable) + cfg.Snapshot store when cfg.DataDir != "",
//     else inmem log/stable + cfg.Snapshot store
//
// This lets tests use inmem transport with real BoltDB persistence to verify
// snapshot/log durability across restarts.
func New(cfg *config.Config, f *fsm.FSM, logger hclog.Logger) (*Node, error) {
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotThreshold = 1024
	raftCfg.SnapshotInterval = 10 * time.Minute
	raftCfg.Logger = logger

	n := &Node{cfg: cfg, fsm: f, logger: logger}

	// --- transport ---
	if cfg.UseInmemTransport {
		// raft v1.7.1: NewInmemTransport returns (ServerAddress, *InmemTransport),
		// not (*InmemTransport, error). A random address is generated when the
		// supplied addr is empty; otherwise it is used as-is.
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(cfg.RaftAddr))
		_ = addr
		n.trans = trans
		raftCfg.HeartbeatTimeout = 200 * time.Millisecond
		raftCfg.ElectionTimeout = 200 * time.Millisecond
		raftCfg.CommitTimeout = 50 * time.Millisecond
		// Default LeaderLeaseTimeout (500ms) must be <= HeartbeatTimeout.
		raftCfg.LeaderLeaseTimeout = 100 * time.Millisecond
	} else {
		trans, err := raft.NewTCPTransport(
			cfg.RaftAddr, nil, 3, 10*time.Second,
			logger.StandardWriter(&hclog.StandardLoggerOptions{}),
		)
		if err != nil {
			return nil, fmt.Errorf("tcp transport: %w", err)
		}
		n.trans = trans
	}

	// --- persistence ---
	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			return nil, fmt.Errorf("mkdir dataDir: %w", err)
		}
		boltStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return nil, fmt.Errorf("bolt store: %w", err)
		}
		n.stable = boltStore
		n.logs = boltStore
		n.bolt = boltStore // release file lock on Shutdown
	} else {
		n.logs = raft.NewInmemStore()
		n.stable = raft.NewInmemStore()
	}
	snaps, err := snapshot.NewStore(cfg.Snapshot, logger)
	if err != nil {
		return nil, err
	}
	n.snaps = snaps

	r, err := raft.NewRaft(raftCfg, f, n.logs, n.stable, n.snaps, n.trans)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}
	n.raft = r
	return n, nil
}

func (n *Node) BootstrapCluster() error {
	servers := make([]raft.Server, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(p.ID),
			Address: raft.ServerAddress(p.Addr),
		})
	}
	cfg := raft.Configuration{Servers: servers}
	fut := n.raft.BootstrapCluster(cfg)
	if err := fut.Error(); err != nil {
		// 已引导过不算错误（重启场景）。
		if errors.Is(err, raft.ErrCantBootstrap) {
			return nil
		}
		return err
	}
	return nil
}

func (n *Node) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	return n.raft.Apply(cmd, timeout)
}

func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

func (n *Node) LeaderAddr() string {
	return string(n.raft.Leader())
}

func (n *Node) State() raft.RaftState {
	return n.raft.State()
}

func (n *Node) Stats() map[string]string {
	return n.raft.Stats()
}

func (n *Node) AddVoter(id, addr string) error {
	fut := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 5*time.Second)
	return fut.Error()
}

func (n *Node) RemoveServer(id string) error {
	fut := n.raft.RemoveServer(raft.ServerID(id), 0, 5*time.Second)
	return fut.Error()
}

// Snapshot forces a snapshot of the local FSM immediately, bypassing the
// SnapshotThreshold gate (raft's runSnapshots only snapshots when log delta
// >= threshold; this calls the user-triggered path). Works on any node —
// snapshot is a local FSM operation, not leader-only. Used by the
// POST /cluster/snapshot API and for periodic log truncation in low-write
// deployments where the threshold is rarely reached.
func (n *Node) Snapshot() error {
	fut := n.raft.Snapshot()
	return fut.Error()
}

func (n *Node) Raft() *raft.Raft { return n.raft }

// Transport returns the underlying raft.Transport. Test harnesses use this to
// wire inmem transports together (InmemTransport peers are isolated until
// Connect is called); production code rarely needs it.
func (n *Node) Transport() raft.Transport { return n.trans }

func (n *Node) Shutdown() error {
	if n.raft == nil {
		return nil
	}
	fut := n.raft.Shutdown()
	err := fut.Error()
	// Release the BoltDB file lock (and any transport resources) so a restart
	// can reopen the same data directory. raft.Raft.Shutdown does not close
	// the backing stores or transport.
	if n.bolt != nil {
		if cErr := n.bolt.Close(); cErr != nil && err == nil {
			err = cErr
		}
		n.bolt = nil
	}
	if closer, ok := n.trans.(interface{ Close() error }); ok {
		if cErr := closer.Close(); cErr != nil && err == nil {
			err = cErr
		}
	}
	return err
}
