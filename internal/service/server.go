// Package service serves complete report snapshots and the GoAccess WebSocket.
package service

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"time"

	"github.com/zekihan/accessrelay/internal/collector"
	"github.com/zekihan/accessrelay/internal/renderer"
	"github.com/zekihan/accessrelay/internal/source"
)

func Handler(c *collector.Collector, r *renderer.Renderer) http.Handler {
	target, _ := url.Parse("http://127.0.0.1:7890")
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "renderer_unavailable", http.StatusServiceUnavailable)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		if !r.Snapshot().Ready {
			http.Error(w, "renderer_unavailable", http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, req)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if !c.Healthy(time.Now()) {
			http.Error(w, "collector_unresponsive", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, req *http.Request) {
		if !r.Snapshot().Ready || !c.Snapshot().Initialized {
			http.Error(w, "initializing", http.StatusServiceUnavailable)
			return
		}
		serveReport(w, req, r.ReportPath())
	})
	mux.HandleFunc("/status.json", func(w http.ResponseWriter, _ *http.Request) {
		s := c.Snapshot()
		s.Heartbeat = float64(time.Now().UnixNano()) / 1e9
		s.Stale = s.LastSuccessfulQuery == nil || s.Heartbeat-*s.LastSuccessfulQuery > configStale(c)
		if s.Cursor != nil {
			if ns, err := source.ParseTimestamp(*s.Cursor); err == nil {
				lag := max(0, time.Since(time.Unix(0, ns)).Seconds())
				s.LagSeconds = &lag
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(struct {
			collector.Status
			Renderer renderer.Snapshot `json:"renderer"`
		}{s, r.Snapshot()})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		s := c.Snapshot()
		rs := r.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		lag := 0.
		if s.LagSeconds != nil {
			lag = *s.LagSeconds
			if s.Cursor != nil {
				if ns, err := source.ParseTimestamp(*s.Cursor); err == nil {
					lag = max(0, time.Since(time.Unix(0, ns)).Seconds())
				}
			}
		}
		healthy := 0
		if rs.Ready {
			healthy = 1
		}
		pressure := 0
		if s.StoragePressure {
			pressure = 1
		}
		_, _ = fmt.Fprintf(w, "accessrelay_queries_total %d\naccessrelay_retries_total %d\naccessrelay_committed_windows_total %d\naccessrelay_response_bytes_total %d\naccessrelay_export_failures_total %d\naccessrelay_cursor_lag_seconds %g\naccessrelay_renderer_ready %d\naccessrelay_renderer_restarts_total %d\naccessrelay_database_bytes %d\naccessrelay_storage_pressure %d\n", s.Queries, s.Retries, s.CommittedWindows, s.CommittedBytes, s.ExportFailures, lag, healthy, rs.Restarts, s.DatabaseBytes, pressure)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" && req.URL.Path != "/index.html" {
			http.Error(w, "report_unavailable", http.StatusServiceUnavailable)
			return
		}
		serveReport(w, req, r.ReportPath())
	})
	return mux
}
func configStale(c *collector.Collector) float64 {
	d, _ := time.ParseDuration(c.Config.Collection.StaleAfter)
	return d.Seconds()
}
func serveReport(w http.ResponseWriter, r *http.Request, path string) {
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "report_unavailable", http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		http.Error(w, "report_unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, "index.html", info.ModTime(), f)
}
