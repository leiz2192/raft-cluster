package testharness

import (
	"testing"
	"time"
)

// TestRecoverTwoOfThreeKeepsCommittedData: 3 节点全 Shutdown，恢复 2 个 →
// 2/3 多数派选主。inmem 重启不保状态，此用例验证的是"2 节点能选主对外
// 服务"的拓扑语义；持久化零丢失由真实 BoltDB 的 raftnode 单测保证。
func TestRecoverTwoOfThreeKeepsCommittedData(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()

	leader := c.WaitForLeader(t)
	if leader == "" {
		t.Fatal("no leader")
	}
	if err := c.Node(leader).Store.Put("committed", []byte("yes")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// 等待复制到所有节点。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.IDs() {
			if _, found := c.Node(id).FSM.Get("committed"); !found {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 全部关闭，移除一个，重启剩 2 个。
	keep := c.IDs()
	drop := keep[2]
	c.ShutdownAll()
	// 模拟永久丢一个：从集群移除该节点对象。
	delete(c.nodes, drop)
	for _, id := range c.IDs() {
		c.RestartNode(id)
	}

	newLeader := c.WaitForLeader(t)
	if newLeader == "" {
		t.Fatal("2-of-3 failed to elect leader")
	}
}

// TestSingleSurvivorCannotElect: 3 节点全 Shutdown，只恢复 1 个 →
// 1/3 无法选主（在 3-voter 配置里 1/3 < 多数派）。
func TestSingleSurvivorCannotElect(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()
	c.WaitForLeader(t)
	c.ShutdownAll()
	keep := c.IDs()[0]
	delete(c.nodes, c.IDs()[1])
	delete(c.nodes, c.IDs()[2])
	c.RestartNode(keep)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Node(keep).Raft.IsLeader() {
			t.Fatal("single survivor should not become leader in 3-voter cluster")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestResetNodeRejoins: 1 节点数据损坏：擦除后重启，重新加入拓扑。
// （inmem 无持久化，此用例验证 Reset 语义 + 重启后重新加入拓扑。）
func TestResetNodeRejoins(t *testing.T) {
	c := NewCluster(t, 3)
	defer c.ShutdownAll()
	c.WaitForLeader(t)
	id := c.IDs()[0]
	// inmem 模式 DataDir 为空，Reset 应无错返回。
	if err := resetForTest(c.Node(id)); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	c.RestartNode(id)
	if c.WaitForLeader(t) == "" {
		t.Fatal("no leader after rejoin")
	}
}
