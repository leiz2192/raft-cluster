package testharness

import (
	"testing"
	"time"
)

func TestThreeNodeClusterElectsLeaderAndReplicates(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()

	leader := c.WaitForLeader(t)
	if leader == "" {
		t.Fatal("no leader elected")
	}

	// Focused assertion: exactly one leader at any instant.
	leaders := 0
	for _, id := range c.IDs() {
		if c.Node(id).Raft.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}

	// 写 leader，验证 3 节点 FSM 一致。
	lid := c.LeaderID()
	s := c.Node(lid).Store
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.IDs() {
			if _, found := c.Node(id).FSM.Get("k"); !found {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("replication did not converge on all nodes")
}

func TestLeaderFailover(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()
	// Election may still be in progress; wait for the first leader before
	// tearing it down so we actually exercise failover, not a cold start.
	old := c.WaitForLeader(t)
	if old == "" {
		t.Fatal("no initial leader")
	}
	c.ShutdownNode(old)
	// 剩 2 节点，应选出新 leader。
	newLeader := c.WaitForLeader(t)
	if newLeader == "" || newLeader == old {
		t.Fatalf("failover failed: old=%s new=%s", old, newLeader)
	}
}
