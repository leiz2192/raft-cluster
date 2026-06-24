package debug

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPprofServed(t *testing.T) {
	srv := NewHTTPServer("127.0.0.1:0")
	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()

	// 首页 200 + 含 profile 链接。
	resp, err := http.Get(ts.URL + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/debug/pprof/ status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "goroutine") {
		t.Errorf("missing goroutine link: %s", body)
	}

	// heap 可采集。
	resp, err = http.Get(ts.URL + "/debug/pprof/heap")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/debug/pprof/heap status = %d, want 200", resp.StatusCode)
	}
}

func TestPprofOnlyPprofRoutes(t *testing.T) {
	// 调试 server 不应服务业务路由。
	srv := NewHTTPServer("127.0.0.1:0")
	ts := httptest.NewServer(srv.Handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/kv/k1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("/kv/k1 on debug server = %d, want 404 (isolation)", resp.StatusCode)
	}
}
