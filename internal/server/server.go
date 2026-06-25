package server

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"raft-meta/internal/api"
	"raft-meta/internal/config"
	"raft-meta/internal/debug"
	"raft-meta/internal/fsm"
	"raft-meta/internal/logging"
	"raft-meta/internal/metrics"
	"raft-meta/internal/raftnode"
	"raft-meta/internal/store"
)

// Run builds and runs a node: raft + HTTP server, blocks until SIGINT/SIGTERM.
func Run(cfg *config.Config) error {
	logger := logging.NewLogger(cfg.Log, cfg.NodeID)

	f := fsm.New()
	n, err := raftnode.New(cfg, f, logger)
	if err != nil {
		return fmt.Errorf("raftnode: %w", err)
	}
	defer n.Shutdown()

	// 写 Apply 超时可配（cfg.Raft.ApplyTimeout），空=5s。
	applyTimeout := 5 * time.Second
	if d := cfg.Raft.ApplyTimeout.D(); d > 0 {
		applyTimeout = d
	}
	s := store.New(n, f, applyTimeout)
	m := metrics.New(n, f)
	s.SetMetrics(m)
	a := api.New(s, n, m)
	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: a.Handler(),
	}

	// pprof 隔离到独立调试端口（业务端口不挂）。cfg.Debug.Addr 空 → 不开。
	var debugSrv *http.Server
	if cfg.Debug.Addr != "" {
		debugSrv = debug.NewHTTPServer(cfg.Debug.Addr)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	if debugSrv != nil {
		go func() {
			logger.Info("debug (pprof) listening", "addr", cfg.Debug.Addr)
			if err := debugSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}()
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutdownCtx)
		if debugSrv != nil {
			debugSrv.Shutdown(shutdownCtx)
		}
		return nil
	}
}

// Init bootstraps the cluster from the given config (call once, on one node).
func Init(cfg *config.Config) error {
	logger := logging.NewLogger(cfg.Log, cfg.NodeID+"-init")
	f := fsm.New()
	n, err := raftnode.New(cfg, f, logger)
	if err != nil {
		return err
	}
	defer n.Shutdown()
	if err := n.BootstrapCluster(); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	fmt.Printf("cluster bootstrapped with %d peers on node %s\n", len(cfg.Peers), cfg.NodeID)
	return nil
}
