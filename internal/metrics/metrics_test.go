package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
	"raft-meta/internal/raftnode"
)

func newLeaderNode(t *testing.T) (*raftnode.Node, *fsm.FSM) {
	t.Helper()
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID: "n1", RaftAddr: "127.0.0.1:7601",
		Peers: []config.Peer{{ID: "n1", Addr: "127.0.0.1:7601"}},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
	n, err := raftnode.New(cfg, f, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Shutdown() })
	if err := n.BootstrapCluster(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !n.IsLeader() {
		time.Sleep(20 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("not leader")
	}
	return n, f
}

// applyPut drives fsm.Apply with a put command (for tests that need FSM state
// without going through raft.Apply).
func applyPut(t *testing.T, f *fsm.FSM, key, val string) {
	t.Helper()
	data, err := fsm.EncodeCommand(&fsm.Command{Op: fsm.OpPut, Key: key, Value: []byte(val)})
	if err != nil {
		t.Fatal(err)
	}
	f.Apply(&raft.Log{Data: data})
}

func TestObserveKVOp(t *testing.T) {
	n, f := newLeaderNode(t)
	m := New(n, f)

	m.ObserveKVOp("put", nil, 5*time.Millisecond)
	m.ObserveKVOp("put", errors.New("fail"), 2*time.Millisecond)
	m.ObserveKVOp("get", nil, time.Millisecond)

	if got := testutil.ToFloat64(m.kvOps.WithLabelValues("put")); got != 2 {
		t.Fatalf("kv_ops put = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.kvErrors.WithLabelValues("put")); got != 1 {
		t.Fatalf("kv_op_errors put = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.kvOps.WithLabelValues("get")); got != 1 {
		t.Fatalf("kv_ops get = %v, want 1", got)
	}
}

func TestObserveSnapshot(t *testing.T) {
	n, f := newLeaderNode(t)
	m := New(n, f)
	m.ObserveSnapshot()
	m.ObserveSnapshot()
	if got := testutil.ToFloat64(m.snapshots); got != 2 {
		t.Fatalf("snapshots = %v, want 2", got)
	}
}

func TestStateCollector(t *testing.T) {
	n, f := newLeaderNode(t)
	applyPut(t, f, "k", "v")

	m := New(n, f)
	got, err := collectText(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"raft_meta_is_leader",
		"raft_meta_fsm_keys",
		"raft_meta_commit_index",
		"raft_meta_peers",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("metrics missing %q\n--- output ---\n%s", want, got)
		}
	}
}

func TestHTTPMiddleware(t *testing.T) {
	n, f := newLeaderNode(t)
	m := New(n, f)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	srv := httptest.NewServer(m.HTTPMiddleware(inner))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/anything")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d, want teapot", resp.StatusCode)
	}
	if got := testutil.ToFloat64(m.httpReqs.WithLabelValues("GET", "418")); got != 1 {
		t.Fatalf("http_requests GET 418 = %v, want 1", got)
	}
}

func TestStatusMap(t *testing.T) {
	n, f := newLeaderNode(t)
	applyPut(t, f, "k", "v")
	m := New(n, f)
	sm := m.StatusMap()
	if sm["is_leader"] != true {
		t.Errorf("is_leader = %v, want true", sm["is_leader"])
	}
	if keys, ok := sm["fsm_keys"].(int); !ok || keys < 1 {
		t.Errorf("fsm_keys = %v, want >=1", sm["fsm_keys"])
	}
	if _, ok := sm["commit_index"]; !ok {
		t.Error("commit_index missing")
	}
}

// collectText gathers all metrics from m's registry in text format.
func collectText(m *Metrics) (string, error) {
	families, err := m.reg.Gather()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, fam := range families {
		b.WriteString(fam.String())
	}
	return b.String(), nil
}
