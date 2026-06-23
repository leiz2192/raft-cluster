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
