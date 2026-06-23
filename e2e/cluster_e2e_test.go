//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// TestE2EWriteReadAndFailover 覆盖 spec §6.4：真实 3 进程，写 leader、读、
// kill leader 后客户端重试落新主，数据仍在。
func TestE2EWriteReadAndFailover(t *testing.T) {
	bin := os.Getenv("RAFT_META_BIN")
	if bin == "" {
		// 默认用 go run 拉起的临时构建产物。
		out, err := exec.Command("go", "build", "-o", t.TempDir()+"/raft-meta", "./cmd/raft-meta").CombinedOutput()
		if err != nil {
			t.Fatalf("build: %v\n%s", err, out)
		}
		bin = t.TempDir() + "/raft-meta"
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("binary not built: %v", err)
	}

	dataRoot := t.TempDir()
	procs, cleanup := startCluster(t, bin, dataRoot)
	defer cleanup()
	for i := 0; i < 3; i++ {
		waitForHTTP(t, i)
	}

	// 找到 leader。
	leaderIdx := -1
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < 3; i++ {
			resp, _ := http.Get(httpBase(i) + "/cluster/status")
			if resp != nil && resp.StatusCode == 200 {
				var s map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&s)
				resp.Body.Close()
				if s["state"] == "Leader" {
					leaderIdx = i
					break
				}
			}
		}
		if leaderIdx >= 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if leaderIdx < 0 {
		t.Fatal("no leader elected")
	}

	// 写 leader。
	body, _ := json.Marshal(map[string]string{"value": "e2e"})
	req, err := http.NewRequest(http.MethodPut, httpBase(leaderIdx)+"/kv/e2ekey", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if r.StatusCode != 200 {
		t.Fatalf("PUT status = %d", r.StatusCode)
	}
	r.Body.Close()

	// 等复制，从 follower 读（本地读，可能脏读，但等一会应一致）。
	time.Sleep(time.Second)
	follower := (leaderIdx + 1) % 3
	got, err := http.Get(httpBase(follower) + "/kv/e2ekey")
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]string
	json.NewDecoder(got.Body).Decode(&v)
	got.Body.Close()
	if v["value"] != "e2e" {
		t.Fatalf("follower read = %q, want e2e", v["value"])
	}

	// kill leader，验证选新主且数据仍在。
	procs[leaderIdx].cmd.Process.Signal(syscall.SIGTERM)
	procs[leaderIdx].cmd.Wait()
	procs[leaderIdx].cmd = nil

	newLeader := -1
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < 3; i++ {
			if i == leaderIdx {
				continue
			}
			resp, _ := http.Get(httpBase(i) + "/cluster/status")
			if resp != nil && resp.StatusCode == 200 {
				var s map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&s)
				resp.Body.Close()
				if s["state"] == "Leader" {
					newLeader = i
					break
				}
			}
		}
		if newLeader >= 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if newLeader < 0 {
		t.Fatal("no new leader after failover")
	}

	got, err = http.Get(httpBase(newLeader) + "/kv/e2ekey")
	if err != nil {
		t.Fatal(err)
	}
	var v2 map[string]string
	json.NewDecoder(got.Body).Decode(&v2)
	got.Body.Close()
	if v2["value"] != "e2e" {
		t.Fatalf("after failover read = %q, want e2e", v2["value"])
	}
}
