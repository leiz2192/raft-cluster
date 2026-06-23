package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"raft-meta/internal/fsm"
	"raft-meta/internal/raftnode"
)

// stateCollector reads live raft/FSM state on each scrape and emits gauges.
// State is read-on-scrape (not incremented) because it comes from raft.Stats()
// and the FSM — there's no increment event to hook.
type stateCollector struct {
	node *raftnode.Node
	fsm  *fsm.FSM

	isLeader          *prometheus.Desc
	term              *prometheus.Desc
	commitIndex       *prometheus.Desc
	appliedIndex      *prometheus.Desc
	lastLogIndex      *prometheus.Desc
	lastSnapshotIndex *prometheus.Desc
	fsmKeys           *prometheus.Desc
	peers             *prometheus.Desc
}

func newStateCollector(n *raftnode.Node, f *fsm.FSM) *stateCollector {
	return &stateCollector{
		node: n,
		fsm:  f,
		isLeader:          prometheus.NewDesc(namespace+"_is_leader", "1 if this node is raft leader, else 0.", nil, nil),
		term:              prometheus.NewDesc(namespace+"_raft_term", "Current raft term.", nil, nil),
		commitIndex:       prometheus.NewDesc(namespace+"_commit_index", "Current raft commit index.", nil, nil),
		appliedIndex:      prometheus.NewDesc(namespace+"_applied_index", "Current raft applied index.", nil, nil),
		lastLogIndex:      prometheus.NewDesc(namespace+"_last_log_index", "Last raft log index.", nil, nil),
		lastSnapshotIndex: prometheus.NewDesc(namespace+"_last_snapshot_index", "Last snapshot index.", nil, nil),
		fsmKeys:           prometheus.NewDesc(namespace+"_fsm_keys", "Number of keys in the FSM.", nil, nil),
		peers:             prometheus.NewDesc(namespace+"_peers", "Number of known voter peers (excluding self).", nil, nil),
	}
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.isLeader
	ch <- c.term
	ch <- c.commitIndex
	ch <- c.appliedIndex
	ch <- c.lastLogIndex
	ch <- c.lastSnapshotIndex
	ch <- c.fsmKeys
	ch <- c.peers
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	if c.node == nil {
		return
	}
	stats := c.node.Stats()
	gauge := func(desc *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v)
	}
	leaderVal := 0.0
	if c.node.IsLeader() {
		leaderVal = 1.0
	}
	gauge(c.isLeader, leaderVal)
	gauge(c.term, parseFloat(stats["term"]))
	gauge(c.commitIndex, parseFloat(stats["commit_index"]))
	gauge(c.appliedIndex, parseFloat(stats["applied_index"]))
	gauge(c.lastLogIndex, parseFloat(stats["last_log_index"]))
	gauge(c.lastSnapshotIndex, parseFloat(stats["last_snapshot_index"]))
	gauge(c.peers, parseFloat(stats["num_peers"]))
	if c.fsm != nil {
		gauge(c.fsmKeys, float64(c.fsm.Len()))
	}
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}
