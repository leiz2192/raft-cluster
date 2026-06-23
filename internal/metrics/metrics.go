package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"raft-meta/internal/fsm"
	"raft-meta/internal/raftnode"
)

const namespace = "raft_meta"

// Metrics holds all instrumentation: KV op counters/histograms, snapshot
// trigger counter, HTTP request counters/histograms, and a custom collector
// that reads live raft/FSM state on scrape. All metrics are registered on a
// private registry so multiple instances (tests) don't collide.
type Metrics struct {
	node *raftnode.Node
	fsm  *fsm.FSM
	reg  *prometheus.Registry

	kvOps     *prometheus.CounterVec   // {op}
	kvErrors  *prometheus.CounterVec   // {op}
	applyDur  *prometheus.HistogramVec // {op} writes
	readDur   *prometheus.HistogramVec // {op} reads
	snapshots prometheus.Counter       // manual triggers via API
	httpReqs  *prometheus.CounterVec   // {method,code}
	httpDur   *prometheus.HistogramVec // {method}
}

// New creates a Metrics bound to node n (for raft stats) and fsm f (for key
// count). nil n/f are allowed (the collector skips nil refs) so tests can
// build partial instances.
func New(n *raftnode.Node, f *fsm.FSM) *Metrics {
	m := &Metrics{
		node: n,
		fsm:  f,
		reg:  prometheus.NewRegistry(),
	}

	m.kvOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "kv_ops_total", Help: "Total KV operations by type.",
	}, []string{"op"})
	m.kvErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "kv_op_errors_total", Help: "Total KV operation errors by type.",
	}, []string{"op"})
	m.applyDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "kv_apply_duration_seconds", Help: "KV write (raft.Apply) duration.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"op"})
	m.readDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "kv_read_duration_seconds", Help: "KV read (local FSM) duration.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"op"})
	m.snapshots = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace, Name: "snapshot_triggers_total", Help: "Manual snapshot triggers via POST /cluster/snapshot.",
	})
	m.httpReqs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace, Name: "http_requests_total", Help: "HTTP requests by method and status code.",
	}, []string{"method", "code"})
	m.httpDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace, Name: "http_request_duration_seconds", Help: "HTTP request duration.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method"})

	m.reg.MustRegister(
		m.kvOps, m.kvErrors, m.applyDur, m.readDur, m.snapshots,
		m.httpReqs, m.httpDur,
		newStateCollector(n, f),
	)
	return m
}

// ObserveKVOp records a KV operation: increments the op counter (and error
// counter if err != nil), and observes the duration — applyDur for writes
// (put/delete), readDur for reads (get/list).
func (m *Metrics) ObserveKVOp(op string, err error, duration time.Duration) {
	if m == nil {
		return
	}
	m.kvOps.WithLabelValues(op).Inc()
	if err != nil {
		m.kvErrors.WithLabelValues(op).Inc()
	}
	sec := duration.Seconds()
	switch op {
	case "put", "delete":
		m.applyDur.WithLabelValues(op).Observe(sec)
	case "get", "list":
		m.readDur.WithLabelValues(op).Observe(sec)
	}
}

// ObserveSnapshot records a manual snapshot trigger.
func (m *Metrics) ObserveSnapshot() {
	if m == nil {
		return
	}
	m.snapshots.Inc()
}

// SnapshotsCounter exposes the manual-snapshot counter for tests.
func (m *Metrics) SnapshotsCounter() prometheus.Counter { return m.snapshots }

// PrometheusHandler returns an http.Handler exposing all metrics in the
// Prometheus exposition format for /metrics.
func (m *Metrics) PrometheusHandler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// HTTPMiddleware wraps next, counting requests by method+status code and
// recording request duration.
func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if m != nil {
			m.httpReqs.WithLabelValues(r.Method, strconv.Itoa(rec.status)).Inc()
			m.httpDur.WithLabelValues(r.Method).Observe(time.Since(start).Seconds())
		}
	})
}

// StatusMap returns monitorable state for the JSON /cluster/status endpoint:
// is_leader, fsm_keys, peers, term, commit_index, applied_index,
// last_snapshot_index. Does not duplicate state/leader (set by the caller).
func (m *Metrics) StatusMap() map[string]interface{} {
	out := map[string]interface{}{}
	if m == nil || m.node == nil {
		return out
	}
	stats := m.node.Stats()
	out["is_leader"] = m.node.IsLeader()
	out["peers"] = parseIntStat(stats["num_peers"])
	out["term"] = parseIntStat(stats["term"])
	out["commit_index"] = parseIntStat(stats["commit_index"])
	out["applied_index"] = parseIntStat(stats["applied_index"])
	out["last_snapshot_index"] = parseIntStat(stats["last_snapshot_index"])
	if m.fsm != nil {
		out["fsm_keys"] = m.fsm.Len()
	}
	return out
}

func parseIntStat(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// statusRecorder wraps http.ResponseWriter to capture the status code.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wrote {
		return
	}
	r.status = code
	r.wrote = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wrote {
		r.wrote = true
	}
	return r.ResponseWriter.Write(b)
}
