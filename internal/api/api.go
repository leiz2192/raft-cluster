package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"raft-meta/internal/metrics"
	"raft-meta/internal/raftnode"
	"raft-meta/internal/store"
)

// API exposes the store and raft node over HTTP.
type API struct {
	store   *store.Store
	node    *raftnode.Node
	metrics *metrics.Metrics // nil-safe: routes still work, /metrics skipped
}

// New builds an API backed by the given store, raft node, and metrics.
func New(s *store.Store, n *raftnode.Node, m *metrics.Metrics) *API {
	return &API{store: s, node: n, metrics: m}
}

// Handler returns the HTTP handler serving /kv, /cluster, and /metrics routes.
// When metrics is wired, /metrics is served and all routes are wrapped in the
// HTTP instrumentation middleware.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", a.handleKV)
	mux.HandleFunc("/kv", a.handleKVList)
	mux.HandleFunc("/cluster/status", a.handleClusterStatus)
	mux.HandleFunc("/cluster/join", a.handleJoin)
	mux.HandleFunc("/cluster/remove", a.handleRemove)
	mux.HandleFunc("/cluster/snapshot", a.handleSnapshot)
	// pprof 已隔离到独立调试端口（见 internal/debug + server.Run），业务端口不挂。
	var h http.Handler = mux
	if a.metrics != nil {
		mux.Handle("/metrics", a.metrics.PrometheusHandler())
		h = a.metrics.HTTPMiddleware(mux)
	}
	return h
}

type kvBody struct {
	Value string `json:"value"`
}

func (a *API) handleKV(w http.ResponseWriter, r *http.Request) {
	// Trim the "/kv" prefix but keep the leading slash of the key, so that
	// hierarchical keys (e.g. /nodes/n1) are stored and listed with their
	// natural slash-prefixed form. A bare "/kv/" request yields key "/" which
	// is treated as missing.
	key := strings.TrimPrefix(r.URL.Path, "/kv")
	if key == "" || key == "/" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		var b kvBody
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil && err != io.EOF {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if err := a.store.Put(key, []byte(b.Value)); err != nil {
			a.writeWriteError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		if err := a.store.Delete(key); err != nil {
			a.writeWriteError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodGet:
		if r.URL.Query().Get("consistent") == "true" && !a.node.IsLeader() {
			a.redirectToLeader(w, r)
			return
		}
		v, ok := a.store.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"value": string(v)})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (a *API) handleKVList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	if r.URL.Query().Get("consistent") == "true" && !a.node.IsLeader() {
		a.redirectToLeader(w, r)
		return
	}
	items := a.store.List(prefix)
	out := make(map[string]string, len(items))
	for k, v := range items {
		out[k] = string(v)
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *API) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("full") == "true" {
		a.handleFullStatus(w, r)
		return
	}
	writeJSON(w, http.StatusOK, a.localStatus())
}

// localStatus builds this node's status map (state/leader/stats + metrics
// extension). Used both for the local /cluster/status response and as the
// "self" entry in the full fan-out.
func (a *API) localStatus() map[string]interface{} {
	stats := a.node.Stats()
	out := map[string]interface{}{
		"nodeID": a.node.ID(),
		"state":  a.node.State().String(),
		"leader": a.node.LeaderAddr(),
		"stats":  stats,
	}
	if a.metrics != nil {
		for k, v := range a.metrics.StatusMap() {
			out[k] = v
		}
	}
	return out
}

// handleFullStatus fans out: returns this node's status plus every peer's
// status, fetched over HTTP from each peer's configured httpAddr. Peers are
// queried with the local (non-full) endpoint, so no recursion. Unreachable
// peers are marked reachable=false with the error.
func (a *API) handleFullStatus(w http.ResponseWriter, r *http.Request) {
	self := a.localStatus()
	self["reachable"] = true
	nodes := map[string]interface{}{a.node.ID(): self}

	for id, httpAddr := range a.node.PeerHTTPAddrs() {
		if id == a.node.ID() {
			continue
		}
		if httpAddr == "" {
			nodes[id] = map[string]interface{}{"reachable": false, "error": "peer http address not configured"}
			continue
		}
		peer, err := fetchPeerStatus(httpAddr)
		if err != nil {
			nodes[id] = map[string]interface{}{"reachable": false, "error": err.Error()}
			continue
		}
		peer["reachable"] = true
		nodes[id] = peer
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"nodes": nodes})
}

// fetchPeerStatus GETs http://httpAddr/cluster/status (local, non-full) and
// decodes the JSON status map. 2s timeout so one slow/dead peer doesn't stall
// the whole fan-out.
func fetchPeerStatus(httpAddr string) (map[string]interface{}, error) {
	url := "http://" + httpAddr + "/cluster/status"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := peerHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s: status %d", httpAddr, resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("peer %s: decode: %w", httpAddr, err)
	}
	return out, nil
}

var peerHTTPClient = &http.Client{Timeout: 2 * time.Second}

type memberBody struct {
	ID       string `json:"id"`
	Addr     string `json:"addr"`     // raft address
	HTTPAddr string `json:"httpAddr"` // HTTP business address (for status fanout / redirects); optional
}

func (a *API) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !a.node.IsLeader() {
		a.redirectToLeader(w, r)
		return
	}
	var b memberBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := a.node.AddVoter(b.ID, b.Addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Track the new peer's HTTP address so /cluster/status?full=true can fan
	// out to it and redirects can map its raft addr → http addr. Best-effort:
	// httpAddr is optional; without it the voter is in raft but not in fanout.
	if b.HTTPAddr != "" {
		if err := a.node.AddDynamicPeer(b.ID, b.Addr, b.HTTPAddr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

func (a *API) handleRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !a.node.IsLeader() {
		a.redirectToLeader(w, r)
		return
	}
	var b memberBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := a.node.RemoveServer(b.ID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Drop the dynamic-peer entry (no-op if it was never dynamic / never added).
	if err := a.node.RemoveDynamicPeer(b.ID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

// handleSnapshot triggers a local FSM snapshot on demand, bypassing the raft
// SnapshotThreshold gate. Works on any node (snapshot is local, not
// leader-only). Useful for periodic log truncation in low-write deployments.
func (a *API) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := a.node.Snapshot(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if a.metrics != nil {
		a.metrics.ObserveSnapshot()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "snapshot taken"})
}

func (a *API) writeWriteError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, store.ErrNotLeader) {
		a.redirectToLeader(w, r)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// redirectToLeader sends 307 to the leader's HTTP address when known,
// else 503. Maps raft address to http address via the configured peers. The
// original request's path and query are preserved in the Location so a
// redirect-following client lands on the same resource on the leader.
func (a *API) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := a.node.LeaderAddr()
	if leader == "" {
		http.Error(w, "no leader (election in progress)", http.StatusServiceUnavailable)
		return
	}
	// Map the leader's raft address to its HTTP address via cfg.Peers; fall
	// back to the raft address if unmapped (e.g. peers lack httpAddr).
	httpAddr := a.node.HTTPAddrForRaft(leader)
	if httpAddr == "" {
		httpAddr = leader
	}
	// Preserve the original path + query so the redirected request targets the
	// same resource. r may be nil only if a future caller bypasses the handler;
	// default to "/" in that case.
	uri := "/"
	if r != nil {
		uri = r.URL.RequestURI()
	}
	// NOTE: Location header MUST be set before WriteHeader — writeJSON below
	// calls WriteHeader(307).
	w.Header().Set("Location", "http://"+httpAddr+uri)
	writeJSON(w, http.StatusTemporaryRedirect, map[string]string{
		"leader": httpAddr,
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
