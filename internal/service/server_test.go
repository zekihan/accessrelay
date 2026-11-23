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
	commonsmetrics "github.com/zekihan/go-commons/metrics"
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

func TestReportServesSameSecondReplacement(t *testing.T) {
	c, r := serviceFixture(t)
	h := Handler(c, r)
	const original = "<html>old</html>"
	const replacement = "<html>new</html>"
	modTime := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	writeSnapshot := func(path, body string, modified time.Time) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, modified, modified); err != nil {
			t.Fatal(err)
		}
	}
	path := r.ReportPath()
	writeSnapshot(path, original, modTime)
	if w := request(h, "/"); w.Code != http.StatusOK || w.Body.String() != original {
		t.Fatalf("original report: %d %q", w.Code, w.Body.String())
	}
	next := path + ".next"
	writeSnapshot(next, replacement, modTime.Add(500*time.Millisecond))
	if err := os.Rename(next, path); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("If-Modified-Since", modTime.Format(http.TimeFormat))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Body.String() != replacement || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("replacement report: %d %v %q", w.Code, w.Header(), w.Body.String())
	}
}

func TestStatusAndMetricsAreSeparated(t *testing.T) {
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
	if w := request(h, "/metrics"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("public listener exposed metrics: %d %q", w.Code, w.Body.String())
	}
	registry := commonsmetrics.New()
	metricsHandler := MetricsHandler(registry, c, r)
	w := httptest.NewRecorder()
	metricsHandler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, name := range []string{"accessrelay_queries_total", "accessrelay_retries_total", "accessrelay_committed_windows_total", "accessrelay_response_bytes_total", "accessrelay_export_failures_total", "accessrelay_cursor_lag_seconds", "accessrelay_renderer_ready", "accessrelay_renderer_restarts_total", "accessrelay_database_bytes", "accessrelay_storage_pressure", "go_goroutines"} {
		if !strings.Contains(w.Body.String(), name) {
			t.Fatalf("metrics omitted %s: %q", name, w.Body.String())
		}
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
	w = httptest.NewRecorder()
	metricsHandler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(w.Body.String(), "accessrelay_cursor_lag_seconds ") {
		t.Fatalf("metrics lag missing: %q", w.Body.String())
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
