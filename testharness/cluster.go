package testharness

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
	"raft-meta/internal/raftnode"
	"raft-meta/internal/store"
)

// Node is a single inmem raft node plus its FSM and Store.
type Node struct {
	ID    string
	Raft  *raftnode.Node
	FSM   *fsm.FSM
	Store *store.Store
	cfg   *config.Config
}

// Cluster manages a set of inmem raft nodes for integration tests.
type Cluster struct {
	t      *testing.T
	nodes  map[string]*Node
	order  []string
	logger hclog.Logger
}

// NewCluster spins up n inmem nodes with a shared peer list and joint
// bootstrap. Each node uses UseInmemTransport and DataDir="" (inmem stores).
// inmem transport addresses are just identifiers (no real TCP binds), but each
// node still needs a distinct address string.
func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	c := &Cluster{t: t, nodes: map[string]*Node{}, logger: hclog.NewNullLogger()}

	peers := make([]config.Peer, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		peers[i] = config.Peer{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", 7400+i+1)}
	}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		cfg := &config.Config{
			NodeID:   id,
			RaftAddr: peers[i].Addr,
			HTTPAddr: fmt.Sprintf("127.0.0.1:%d", 8400+i+1),
			Peers:    peers,
			Snapshot:          config.SnapshotConfig{Type: "inmem"},
			UseInmemTransport: true,
		}
		f := fsm.New()
		node, err := raftnode.New(cfg, f, c.logger)
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		if err := node.BootstrapCluster(); err != nil {
			t.Fatalf("bootstrap %s: %v", id, err)
		}
		nd := &Node{ID: id, Raft: node, FSM: f, Store: store.New(node, f, 2*time.Second), cfg: cfg}
		c.nodes[id] = nd
		c.order = append(c.order, id)
	}

	c.connectTransports()
	return c
}

// connectTransports wires each node's InmemTransport to every other node's.
// hashicorp/raft's InmemTransport peers are isolated by default: an AppendEntries
// or RequestVote to a peer only succeeds after Connect registered that peer's
// transport. Without this, a multi-node inmem cluster can never reach quorum.
func (c *Cluster) connectTransports() {
	for i, a := range c.order {
		na, ok := c.nodes[a]
		if !ok || na == nil {
			continue
		}
		ta, ok := na.Raft.Transport().(*raft.InmemTransport)
		if !ok {
			continue
		}
		for j, b := range c.order {
			if i == j {
				continue
			}
			nb, ok := c.nodes[b]
			if !ok || nb == nil {
				continue
			}
			tb, ok := nb.Raft.Transport().(*raft.InmemTransport)
			if !ok {
				continue
			}
			ta.Connect(raft.ServerAddress(nb.cfg.RaftAddr), tb)
		}
	}
}

// IDs returns the node IDs in creation order.
func (c *Cluster) IDs() []string { return c.order }

// Node returns the node with the given ID (nil if absent).
func (c *Cluster) Node(id string) *Node { return c.nodes[id] }

// LeaderID returns the ID of the current leader, or "" if none.
func (c *Cluster) LeaderID() string {
	for _, id := range c.order {
		if n, ok := c.nodes[id]; ok && n.Raft.IsLeader() {
			return id
		}
	}
	return ""
}

// WaitForLeader polls up to ~5s for any node's IsLeader() to become true.
func (c *Cluster) WaitForLeader(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.order {
			if n, ok := c.nodes[id]; ok && n.Raft.IsLeader() {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ""
}

// ShutdownNode shuts down a single node's raft instance (and transport).
func (c *Cluster) ShutdownNode(id string) {
	if n, ok := c.nodes[id]; ok {
		_ = n.Raft.Shutdown()
	}
}

// RestartNode rebuilds the node with the same config. inmem stores do not
// persist state, so this helper is only for topology/election tests — NOT
// persistence recovery (covered by raftnode's BoltDB test).
func (c *Cluster) RestartNode(id string) {
	c.t.Helper()
	old := c.nodes[id]
	if old == nil {
		return
	}
	_ = old.Raft.Shutdown()
	f := fsm.New()
	node, err := raftnode.New(old.cfg, f, c.logger)
	if err != nil {
		c.t.Fatalf("restart %s: %v", id, err)
	}
	// 已引导过的配置：BootstrapCluster 会返回 ErrCantBootstrap，被吞掉。
	if err := node.BootstrapCluster(); err != nil {
		c.t.Fatalf("restart bootstrap %s: %v", id, err)
	}
	c.nodes[id] = &Node{ID: id, Raft: node, FSM: f, Store: store.New(node, f, 2*time.Second), cfg: old.cfg}
	// Re-wire this node's fresh transport to the surviving peers.
	if _, ok := node.Transport().(*raft.InmemTransport); ok {
		c.connectTransports()
	}
}

// ShutdownAll shuts down every node in the cluster.
func (c *Cluster) ShutdownAll() {
	for _, n := range c.nodes {
		_ = n.Raft.Shutdown()
	}
}

// resetForTest wipes a node's data dir (no-op for inmem) for DR tests.
func resetForTest(n *Node) error {
	return raftnodeReset(n.cfg.DataDir)
}
