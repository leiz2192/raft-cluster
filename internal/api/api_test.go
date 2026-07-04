package api

import (
	"bytes"
	"encoding/json"
	"io"
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
	"raft-meta/internal/metrics"
	"raft-meta/internal/raftnode"
	"raft-meta/internal/store"
)

func newAPI(t *testing.T) (*API, *fsm.FSM) {
	t.Helper()
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID: "n1", RaftAddr: "127.0.0.1:7201", HTTPAddr: "127.0.0.1:8201",
		Peers: []config.Peer{{ID: "n1", Addr: "127.0.0.1:7201"}},
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
	s := store.New(n, f, 2*time.Second)
	m := metrics.New(n, f)
	s.SetMetrics(m)
	return New(s, n, m), f
}

func TestPutAndGet(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	body, _ := json.Marshal(map[string]string{"value": "hello"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/k1", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(srv.URL + "/kv/k1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]string
	json.NewDecoder(resp.Body).Decode(&got)
	if got["value"] != "hello" {
		t.Fatalf("GET value = %q, want hello", got["value"])
	}
}

func TestList(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	for _, k := range []string{"/nodes/n1", "/nodes/n2", "/svc/s1"} {
		body, _ := json.Marshal(map[string]string{"value": "v"})
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv"+k, bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	resp, err := http.Get(srv.URL + "/kv?prefix=/nodes/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]string
	json.NewDecoder(resp.Body).Decode(&got)
	if len(got) != 2 {
		t.Fatalf("list len = %d, want 2; got %v", len(got), got)
	}
}

func TestDelete(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	body, _ := json.Marshal(map[string]string{"value": "v"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/k1", bytes.NewReader(body))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodDelete, srv.URL+"/kv/k1", nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(srv.URL + "/kv/k1")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestClusterStatus(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/cluster/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&got)
	if got["state"] != "Leader" {
		t.Fatalf("state = %v, want Leader", got["state"])
	}
	if !strings.Contains(got["leader"].(string), "7201") {
		t.Fatalf("leader = %v", got["leader"])
	}
}

// TestGetMissingKey asserts 404 for a key that was never written.
func TestGetMissingKey(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/kv/does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET missing key status = %d, want 404", resp.StatusCode)
	}
}

// TestKeyRequired asserts 400 when the key segment is empty (/kv/).
func TestKeyRequired(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/kv/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET /kv/ status = %d, want 400", resp.StatusCode)
	}
}

// TestMethodNotAllowed asserts 405 for unsupported methods on /kv/{key}.
func TestMethodNotAllowed(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	resp, err := http.NewRequest(http.MethodPatch, srv.URL+"/kv/k1", nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.DefaultClient.Do(resp)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PATCH status = %d, want 405", r.StatusCode)
	}
}

// TestSnapshotTrigger 验证 POST /cluster/snapshot 主动触发快照（绕过阈值）。
// 写一条日志后手动触发，断言 raft last_snapshot_index 从 0 变为非 0。
func TestSnapshotTrigger(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	// 先写一条，保证有已应用日志可被快照。
	body, _ := json.Marshal(map[string]string{"value": "snap"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/snapkey", bytes.NewReader(body))
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", r.StatusCode)
	}
	r.Body.Close()

	if got := api.node.Stats()["last_snapshot_index"]; got != "0" {
		t.Fatalf("last_snapshot_index before trigger = %q, want 0", got)
	}

	// 主动触发快照。
	resp, err := http.Post(srv.URL+"/cluster/snapshot", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /cluster/snapshot status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// 快照完成后 last_snapshot_index 应变为非 0（轮询防 racy 读）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if api.node.Stats()["last_snapshot_index"] != "0" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("last_snapshot_index still 0 after trigger; stats=%v", api.node.Stats())
}

// TestMetricsEndpoint 验证 /metrics 暴露 Prometheus 格式指标，含关键名。
func TestMetricsEndpoint(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	// 触发一些操作，让计数器非零。
	body, _ := json.Marshal(map[string]string{"value": "v"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/m1", bytes.NewReader(body))
	r, _ := http.DefaultClient.Do(req)
	r.Body.Close()
	http.Get(srv.URL + "/kv/m1")

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/metrics status = %d", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	s := string(data)
	for _, want := range []string{
		"raft_meta_kv_ops_total",
		"raft_meta_is_leader",
		"raft_meta_fsm_keys",
		"raft_meta_http_requests_total",
		"raft_meta_commit_index",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}

// TestClusterStatusEnriched 验证 /cluster/status 含 metrics 扩展字段。
func TestClusterStatusEnriched(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/cluster/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&got)
	for _, k := range []string{"is_leader", "fsm_keys", "peers", "commit_index"} {
		if _, ok := got[k]; !ok {
			t.Errorf("/cluster/status missing %q; got keys: %v", k, got)
		}
	}
}

// TestSnapshotIncrementsCounter 验证手动快照递增 snapshot_triggers_total。
func TestSnapshotIncrementsCounter(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	body, _ := json.Marshal(map[string]string{"value": "v"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/sc", bytes.NewReader(body))
	r, _ := http.DefaultClient.Do(req)
	r.Body.Close()

	before := testutil.ToFloat64(api.metrics.SnapshotsCounter())
	resp, err := http.Post(srv.URL+"/cluster/snapshot", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	after := testutil.ToFloat64(api.metrics.SnapshotsCounter())
	if after != before+1 {
		t.Fatalf("snapshot counter %v -> %v, want +1", before, after)
	}
}

// TestPprofNotOnBusinessPort 验证 pprof 已从业务端口隔离（业务端口不挂 /debug/pprof/）。
func TestPprofNotOnBusinessPort(t *testing.T) {
	api, _ := newAPI(t)
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/debug/pprof/heap")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("/debug/pprof/heap on business port = %d, want 404 (pprof isolated to debug port)", resp.StatusCode)
	}
}

// TestClusterStatusFullFanout 验证 /cluster/status?full=true 扇出聚合所有节点：
// 本节点 + 2 个 stub peer（返回 follower 状态），都可达。
func TestClusterStatusFullFanout(t *testing.T) {
	stubStatus := func(id string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"nodeID": id, "state": "Follower", "leader": "127.0.0.1:7701",
			})
		}
	}
	stub2 := httptest.NewServer(stubStatus("node2"))
	defer stub2.Close()
	stub3 := httptest.NewServer(stubStatus("node3"))
	defer stub3.Close()

	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID: "node1", RaftAddr: "127.0.0.1:7701", HTTPAddr: "127.0.0.1:8701",
		Peers: []config.Peer{
			{ID: "node1", Addr: "127.0.0.1:7701", HTTPAddr: "127.0.0.1:8701"},
			{ID: "node2", Addr: "127.0.0.1:7702", HTTPAddr: strings.TrimPrefix(stub2.URL, "http://")},
			{ID: "node3", Addr: "127.0.0.1:7703", HTTPAddr: strings.TrimPrefix(stub3.URL, "http://")},
		},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
	n, err := raftnode.New(cfg, f, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Shutdown() })
	m := metrics.New(n, f)
	s := store.New(n, f, 2*time.Second)
	s.SetMetrics(m)
	a := New(s, n, m)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/cluster/status?full=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Nodes map[string]map[string]interface{} `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"node1", "node2", "node3"} {
		node, ok := got.Nodes[id]
		if !ok {
			t.Errorf("nodes missing %q; got keys: %v", id, got.Nodes)
			continue
		}
		if node["reachable"] != true {
			t.Errorf("node %q reachable = %v, want true (err=%v)", id, node["reachable"], node["error"])
		}
	}
	if got.Nodes["node2"]["state"] != "Follower" {
		t.Errorf("node2 state = %v, want Follower", got.Nodes["node2"]["state"])
	}
}

// TestClusterStatusFullUnreachable 验证不可达 peer 标记 reachable=false + error。
func TestClusterStatusFullUnreachable(t *testing.T) {
	log := hclog.NewNullLogger()
	f := fsm.New()
	cfg := &config.Config{
		NodeID: "node1", RaftAddr: "127.0.0.1:7711", HTTPAddr: "127.0.0.1:8711",
		Peers: []config.Peer{
			{ID: "node1", Addr: "127.0.0.1:7711", HTTPAddr: "127.0.0.1:8711"},
			{ID: "node2", Addr: "127.0.0.1:7712", HTTPAddr: "127.0.0.1:1"}, // 不可达端口
			{ID: "node3", Addr: "127.0.0.1:7713", HTTPAddr: "127.0.0.1:2"},
		},
		Snapshot:          config.SnapshotConfig{Type: "inmem"},
		UseInmemTransport: true,
	}
	n, err := raftnode.New(cfg, f, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Shutdown() })
	m := metrics.New(n, f)
	s := store.New(n, f, 2*time.Second)
	s.SetMetrics(m)
	a := New(s, n, m)
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/cluster/status?full=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got struct {
		Nodes map[string]map[string]interface{} `json:"nodes"`
	}
	json.NewDecoder(resp.Body).Decode(&got)
	if got.Nodes["node1"]["reachable"] != true {
		t.Errorf("self node1 reachable = %v, want true", got.Nodes["node1"]["reachable"])
	}
	for _, id := range []string{"node2", "node3"} {
		if got.Nodes[id]["reachable"] == true {
			t.Errorf("node %q should be unreachable (port 1/2), got reachable=true", id)
		}
		if got.Nodes[id]["error"] == nil || got.Nodes[id]["error"] == "" {
			t.Errorf("node %q should have error", id)
		}
	}
}

// newFollowerCluster spins up a 3-node inmem raft cluster, waits for a leader,
// and returns an API wired around a follower node, plus the leader's configured
// HTTP address. Used to exercise the non-leader redirect path through the real
// HTTP handler. The caller must call the returned cleanup func.
func newFollowerCluster(t *testing.T) (*API, string, func()) {
	t.Helper()
	log := hclog.NewNullLogger()
	peers := []config.Peer{
		{ID: "n1", Addr: "127.0.0.1:7301", HTTPAddr: "127.0.0.1:8301"},
		{ID: "n2", Addr: "127.0.0.1:7302", HTTPAddr: "127.0.0.1:8302"},
		{ID: "n3", Addr: "127.0.0.1:7303", HTTPAddr: "127.0.0.1:8303"},
	}
	type nodeFSM struct {
		n *raftnode.Node
		f *fsm.FSM
	}
	nodes := make([]nodeFSM, 3)
	for i := range nodes {
		cfg := &config.Config{
			NodeID: peers[i].ID, RaftAddr: peers[i].Addr, HTTPAddr: peers[i].HTTPAddr,
			Peers:             peers,
			Snapshot:          config.SnapshotConfig{Type: "inmem"},
			UseInmemTransport: true,
		}
		f := fsm.New()
		n, err := raftnode.New(cfg, f, log)
		if err != nil {
			t.Fatalf("node %s: %v", peers[i].ID, err)
		}
		if err := n.BootstrapCluster(); err != nil {
			t.Fatalf("bootstrap %s: %v", peers[i].ID, err)
		}
		nodes[i] = nodeFSM{n, f}
	}
	// Wire inmem transports together so the cluster can reach quorum.
	for i, a := range nodes {
		ta := a.n.Transport().(*raft.InmemTransport)
		for j, b := range nodes {
			if i == j {
				continue
			}
			tb := b.n.Transport().(*raft.InmemTransport)
			ta.Connect(raft.ServerAddress(peers[j].Addr), tb)
		}
	}
	cleanup := func() {
		for _, x := range nodes {
			_ = x.n.Shutdown()
		}
	}

	// Wait for a leader.
	deadline := time.Now().Add(5 * time.Second)
	var leader *raftnode.Node
	for time.Now().Before(deadline) {
		for _, x := range nodes {
			if x.n.IsLeader() {
				leader = x.n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Pick a follower and build its API.
	var follower *raftnode.Node
	var followerFSM *fsm.FSM
	for _, x := range nodes {
		if !x.n.IsLeader() {
			follower, followerFSM = x.n, x.f
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}
	leaderHTTP := follower.HTTPAddrForRaft(follower.LeaderAddr())
	if leaderHTTP == "" {
		t.Fatalf("follower LeaderAddr=%q did not map to a peer HTTP addr", follower.LeaderAddr())
	}
	s := store.New(follower, followerFSM, 2*time.Second)
	m := metrics.New(follower, followerFSM)
	s.SetMetrics(m)
	return New(s, follower, m), leaderHTTP, cleanup
}

// TestFollowerWriteRedirectPreservesPathAndQuery asserts that a PUT to a
// follower responds 307 with a Location that preserves the original path and
// query, so a redirect-following client lands on the same resource on the
// leader instead of the root.
func TestFollowerWriteRedirectPreservesPathAndQuery(t *testing.T) {
	a, leaderHTTP, cleanup := newFollowerCluster(t)
	defer cleanup()
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	// Client that does NOT auto-follow, so we can inspect the 307.
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	body, _ := json.Marshal(map[string]string{"value": "v"})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/kv/k1?ttl=30", bytes.NewReader(body))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("PUT on follower status = %d, want 307", resp.StatusCode)
	}
	want := "http://" + leaderHTTP + "/kv/k1?ttl=30"
	if got := resp.Header.Get("Location"); got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
}

// TestFollowerConsistentReadRedirectPreservesPathAndQuery asserts the same for
// a ?consistent=true GET on a follower (read-side redirect path).
func TestFollowerConsistentReadRedirectPreservesPathAndQuery(t *testing.T) {
	a, leaderHTTP, cleanup := newFollowerCluster(t)
	defer cleanup()
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Get(srv.URL + "/kv/k1?consistent=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("consistent GET on follower status = %d, want 307", resp.StatusCode)
	}
	want := "http://" + leaderHTTP + "/kv/k1?consistent=true"
	if got := resp.Header.Get("Location"); got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
}
