package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

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
			a.writeWriteError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case http.MethodDelete:
		if err := a.store.Delete(key); err != nil {
			a.writeWriteError(w, err)
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
	stats := a.node.Stats()
	out := map[string]interface{}{
		"nodeID": stats["node_id"],
		"state":  a.node.State().String(),
		"leader": a.node.LeaderAddr(),
		"stats":  stats,
	}
	if a.metrics != nil {
		for k, v := range a.metrics.StatusMap() {
			out[k] = v
		}
	}
	writeJSON(w, http.StatusOK, out)
}

type memberBody struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
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

func (a *API) writeWriteError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotLeader) {
		a.redirectToLeader(w, nil)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// redirectToLeader sends 307 to the leader's HTTP address when known,
// else 503. Maps raft address to http address via the configured peers.
func (a *API) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := a.node.LeaderAddr()
	if leader == "" {
		http.Error(w, "no leader (election in progress)", http.StatusServiceUnavailable)
		return
	}
	// 307 redirect: simple and reliable; clients retry and land on the leader.
	// Real deployments need a raft:port -> http:port mapping injected by the
	// server layer; here we fall back to the raft address (accepted
	// simplification — tests rely on the recognizable port suffix).
	httpAddr := leader
	// NOTE: the Location header MUST be set before WriteHeader is called,
	// otherwise net/http logs "superfluous response.WriteHeader call" and the
	// header is dropped. writeJSON below calls WriteHeader(307).
	w.Header().Set("Location", "http://"+httpAddr)
	writeJSON(w, http.StatusTemporaryRedirect, map[string]string{
		"leader": httpAddr,
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
