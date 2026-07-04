package raftnode

import (
	"path/filepath"

	"raft-meta/internal/config"
)

// AddDynamicPeer records a runtime-added peer (via /cluster/join) so it
// appears in PeerHTTPAddrs / HTTPAddrForRaft and survives restart. It is
// best-effort w.r.t. raft membership: call after AddVoter succeeds.
func (n *Node) AddDynamicPeer(id, raftAddr, httpAddr string) error {
	return n.dynPeers.Add(config.Peer{ID: id, Addr: raftAddr, HTTPAddr: httpAddr})
}

// RemoveDynamicPeer drops a runtime-added peer by ID. No-op if absent. Call
// after RemoveServer succeeds; safe even if the peer was never dynamic.
func (n *Node) RemoveDynamicPeer(id string) error {
	return n.dynPeers.Remove(id)
}

// dynamicPeersPath returns the on-disk path for the dynamic-peer store, or ""
// when there is no dataDir (in-memory tests).
func dynamicPeersPath(cfg *config.Config) string {
	if cfg.DataDir == "" {
		return ""
	}
	return filepath.Join(cfg.DataDir, "peers.json")
}
