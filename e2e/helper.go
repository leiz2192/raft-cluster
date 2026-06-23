package e2e

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// proc wraps a running raft-meta node process.
type proc struct {
	cmd *exec.Cmd
	id  string
}

// startCluster builds the binary once and starts 3 nodes with the given
// temporary data root. Returns the 3 procs and a cleanup func.
func startCluster(t *testing.T, bin string, dataRoot string) ([]*proc, func()) {
	t.Helper()
	// 写 3 份临时配置（端口用 9001-3 raft / 10001-3 http，避免与开发端口冲突）。
	cfgs := make([]string, 3)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node%d", i+1)
		dir := filepath.Join(dataRoot, id)
		os.MkdirAll(filepath.Join(dir, "snaps"), 0755)
		path := filepath.Join(dataRoot, id+".yaml")
		content := fmt.Sprintf(`
nodeID: %s
raftAddr: 127.0.0.1:%d
httpAddr: 127.0.0.1:%d
dataDir: %s
peers:
  - {id: node1, addr: 127.0.0.1:9001}
  - {id: node2, addr: 127.0.0.1:9002}
  - {id: node3, addr: 127.0.0.1:9003}
snapshot:
  type: file
  path: %s/snaps
  retain: 3
`, id, 9001+i, 10001+i, dir, dir)
		os.WriteFile(path, []byte(content), 0644)
		cfgs[i] = path
	}

	// 首次部署：在 node1 引导一次。
	if err := exec.Command(bin, "init", "-config", cfgs[0]).Run(); err != nil {
		t.Fatalf("init: %v", err)
	}

	procs := make([]*proc, 3)
	for i, cfg := range cfgs {
		c := exec.Command(bin, "start", "-config", cfg)
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if err := c.Start(); err != nil {
			t.Fatalf("start node%d: %v", i+1, err)
		}
		procs[i] = &proc{cmd: c, id: fmt.Sprintf("node%d", i+1)}
	}
	cleanup := func() {
		for _, p := range procs {
			if p.cmd != nil && p.cmd.Process != nil {
				p.cmd.Process.Signal(syscall.SIGTERM)
				p.cmd.Wait()
			}
		}
		os.RemoveAll(dataRoot)
	}
	return procs, cleanup
}

// httpBase returns the HTTP base URL for node index i (0-based).
func httpBase(i int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", 10001+i)
}

// waitForHTTP polls until the node responds on /cluster/status or times out.
func waitForHTTP(t *testing.T, i int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := httpGet(httpBase(i) + "/cluster/status")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d did not come up", i+1)
}

// httpGet is a thin wrapper around http.Get.
func httpGet(url string) (*http.Response, error) { return http.Get(url) }
