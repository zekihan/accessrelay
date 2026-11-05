package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/collector"
	"github.com/zekihan/accessrelay/internal/config"
	"github.com/zekihan/accessrelay/internal/renderer"
)

func serviceFixture(t *testing.T) (*collector.Collector, *renderer.Renderer) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StateDir = filepath.Join(dir, "state")
	cfg.RuntimeDir = filepath.Join(dir, "runtime")
	cfg.Collection.Source.Cluster = "test"
	c, err := collector.New(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Store.Close(); err != nil {
			t.Error(err)
		}
	})
	r, err := renderer.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, r
}

func request(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestHandlerRoutes(t *testing.T) {
	c, r := serviceFixture(t)
	h := Handler(c, r)
	for _, tc := range []struct {
		path string
		code int
		body string
	}{
		{"/health", 200, "ok\n"}, {"/ready", 503, "initializing\n"},
		{"/ws", 503, "renderer_unavailable\n"}, {"/", 503, "report_unavailable\n"},
		{"/index.html", 503, "report_unavailable\n"}, {"/other", 503, "report_unavailable\n"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			w := request(h, tc.path)
			if w.Code != tc.code || w.Body.String() != tc.body {
				t.Fatalf("got %d %q", w.Code, w.Body.String())
			}
		})
	}
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("Origin", "https://elsewhere.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 403 || w.Body.String() != "origin_forbidden\n" {
		t.Fatalf("origin: %d %q", w.Code, w.Body.String())
	}
	if err := os.WriteFile(r.ReportPath(), []byte("<html>report</html>"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/index.html"} {
		w := request(h, path)
		if w.Code != 200 || w.Body.String() != "<html>report</html>" || w.Header().Get("Content-Type") != "text/html; charset=utf-8" || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("%s: %d %v %q", path, w.Code, w.Header(), w.Body.String())
		}
	}
	unhealthy := &collector.Collector{Config: c.Config}
	w = request(Handler(unhealthy, r), "/health")
	if w.Code != 503 || w.Body.String() != "collector_unresponsive\n" {
		t.Fatalf("health: %d %q", w.Code, w.Body.String())
	}
}

func TestStatusAndMetrics(t *testing.T) {
	c, r := serviceFixture(t)
	h := Handler(c, r)
	checkStatus := func(stale bool, lag bool) {
		t.Helper()
		w := request(h, "/status.json")
		var s collector.Status
		if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("Cache-Control") != "no-store" || s.Stale != stale || (s.LagSeconds != nil) != lag {
			t.Fatalf("status: %d %v %s", w.Code, w.Header(), w.Body.String())
		}
		if lag && (*s.LagSeconds < 9 || *s.LagSeconds > 30) {
			t.Fatalf("lag: %v", *s.LagSeconds)
		}
	}
	checkStatus(true, false)
	w := request(h, "/metrics")
	expected := "accessrelay_queries_total 0\naccessrelay_retries_total 0\naccessrelay_committed_windows_total 0\naccessrelay_response_bytes_total 0\naccessrelay_export_failures_total 0\naccessrelay_cursor_lag_seconds 0\naccessrelay_renderer_ready 0\naccessrelay_renderer_restarts_total 0\naccessrelay_database_bytes 0\naccessrelay_storage_pressure 0\n"
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/plain; version=0.0.4" || w.Body.String() != expected {
		t.Fatalf("metrics: %d %v %q", w.Code, w.Header(), w.Body.String())
	}
	now := time.Now()
	_, err := c.Store.DB.Exec(`INSERT INTO metadata(key,value) VALUES('cursor',?),('last_success',?)`, strconv.FormatInt(now.Add(-10*time.Second).UnixNano(), 10), fmt.Sprintf("%.9f", float64(now.UnixNano())/1e9))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Refresh(now.Add(-5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	checkStatus(false, true)
	w = request(h, "/metrics")
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	if len(lines) != 10 {
		t.Fatalf("metrics lines: %q", w.Body.String())
	}
	lag, err := strconv.ParseFloat(strings.TrimPrefix(lines[5], "accessrelay_cursor_lag_seconds "), 64)
	if err != nil || lag < 9 || lag > 30 {
		t.Fatalf("metrics lag: %q", lines[5])
	}
	c.Config.Collection.StaleAfter = "1s"
	_, err = c.Store.DB.Exec(`UPDATE metadata SET value=? WHERE key='last_success'`, fmt.Sprintf("%.9f", float64(now.Add(-10*time.Second).UnixNano())/1e9))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Refresh(now); err != nil {
		t.Fatal(err)
	}
	checkStatus(true, true)
}

type failingReportFile struct{ *strings.Reader }

func (f failingReportFile) Stat() (os.FileInfo, error) { return nil, errors.New("stat failed") }

func TestServeReportStatFailure(t *testing.T) {
	w := httptest.NewRecorder()
	serveReportFile(w, httptest.NewRequest(http.MethodGet, "/", nil), failingReportFile{strings.NewReader("")})
	if w.Code != http.StatusServiceUnavailable || w.Body.String() != "report_unavailable\n" {
		t.Fatalf("stat failure: %d %q", w.Code, w.Body.String())
	}
}
