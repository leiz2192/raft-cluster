package debug

import (
	"net/http"
	"net/http/pprof"
)

// NewHTTPServer returns an http.Server serving the standard net/http/pprof
// endpoints on addr. Intended for a SEPARATE debug port, isolated from the
// business API — so pprof (and its CPU/heap capture load) never touches the
// /kv, /cluster, /metrics traffic, and can be firewalled independently.
//
// pprof.Index is registered at "/debug/pprof/" and also serves the sub-paths
// (heap/goroutine/allocs/block/mutex/etc.).
func NewHTTPServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &http.Server{Addr: addr, Handler: mux}
}
