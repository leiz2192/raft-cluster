package server

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"raft-meta/internal/api"
	"raft-meta/internal/config"
	"raft-meta/internal/fsm"
	"raft-meta/internal/metrics"
	"raft-meta/internal/raftnode"
	"raft-meta/internal/store"
)

// Run builds and runs a node: raft + HTTP server, blocks until SIGINT/SIGTERM.
func Run(cfg *config.Config) error {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  cfg.NodeID,
		Level: hclog.Info,
	})

	f := fsm.New()
	n, err := raftnode.New(cfg, f, logger)
	if err != nil {
		return fmt.Errorf("raftnode: %w", err)
	}
	defer n.Shutdown()

	s := store.New(n, f, 5*time.Second)
	m := metrics.New(n, f)
	s.SetMetrics(m)
	a := api.New(s, n, m)
	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: a.Handler(),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpSrv.Shutdown(shutdownCtx)
		return nil
	}
}

// Init bootstraps the cluster from the given config (call once, on one node).
func Init(cfg *config.Config) error {
	logger := hclog.New(&hclog.LoggerOptions{Name: cfg.NodeID + "-init", Level: hclog.Info})
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
