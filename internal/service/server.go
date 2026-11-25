// Package service serves complete report snapshots and the GoAccess WebSocket.
package service

import (
	"encoding/json"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	commonsmetrics "github.com/zekihan/go-commons/metrics"

	"github.com/zekihan/accessrelay/internal/collector"
	"github.com/zekihan/accessrelay/internal/renderer"
	"github.com/zekihan/accessrelay/internal/source"
)

func MetricsHandler(
	registry *commonsmetrics.Registry,
	c *collector.Collector,
	r *renderer.Renderer,
) http.Handler {
	queries := registry.Counter("accessrelay_queries_total", nil)
	retries := registry.Counter("accessrelay_retries_total", nil)
	committedWindows := registry.Counter("accessrelay_committed_windows_total", nil)
	responseBytes := registry.Counter("accessrelay_response_bytes_total", nil)
	exportFailures := registry.Counter("accessrelay_export_failures_total", nil)
	rendererRestarts := registry.Counter("accessrelay_renderer_restarts_total", nil)
	lag := registry.Gauge("accessrelay_cursor_lag_seconds", nil)
	ready := registry.Gauge("accessrelay_renderer_ready", nil)
	databaseBytes := registry.Gauge("accessrelay_database_bytes", nil)
	storagePressure := registry.Gauge("accessrelay_storage_pressure", nil)
	var previous struct {
		sync.Mutex

		queries, retries, committedWindows, responseBytes, exportFailures, rendererRestarts uint64
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s := c.Snapshot()
		rs := r.Snapshot()
		previous.Lock()
		queries.Add(delta(s.Queries, previous.queries))
		retries.Add(delta(s.Retries, previous.retries))
		committedWindows.Add(delta(s.CommittedWindows, previous.committedWindows))
		responseBytes.Add(delta(s.CommittedBytes, previous.responseBytes))
		exportFailures.Add(delta(s.ExportFailures, previous.exportFailures))
		rendererRestarts.Add(delta(rs.Restarts, previous.rendererRestarts))
		previous.queries, previous.retries = s.Queries, s.Retries
		previous.committedWindows, previous.responseBytes = s.CommittedWindows, s.CommittedBytes
		previous.exportFailures, previous.rendererRestarts = s.ExportFailures, rs.Restarts
		previous.Unlock()
		lagSeconds := 0.0
		if s.Cursor != nil {
			if ns, err := source.ParseTimestamp(*s.Cursor); err == nil {
				lagSeconds = max(0, time.Since(time.Unix(0, ns)).Seconds())
			}
		}
		lag.Set(lagSeconds)
		if rs.Ready {
			ready.Set(1)
		} else {
			ready.Set(0)
		}
		databaseBytes.Set(float64(s.DatabaseBytes))
		if s.StoragePressure {
			storagePressure.Set(1)
		} else {
			storagePressure.Set(0)
		}
		registry.Handler().ServeHTTP(w, req)
	})
}

func Handler(c *collector.Collector, r *renderer.Renderer) http.Handler {
	target, err := url.Parse("http://127.0.0.1:7890")
	if err != nil {
		panic(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorLog = log.New(io.Discard, "", 0)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "renderer_unavailable", http.StatusServiceUnavailable)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		if !trustedOrigin(req, r.Config.Report.WebsocketURL) {
			http.Error(w, "origin_forbidden", http.StatusForbidden)
			return
		}
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
		if _, err := io.WriteString(w, "ok\n"); err != nil {
			return
		}
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
		s.Stale = s.LastSuccessfulQuery == nil ||
			s.Heartbeat-*s.LastSuccessfulQuery > configStale(c)
		if s.Cursor != nil {
			if ns, err := source.ParseTimestamp(*s.Cursor); err == nil {
				lag := max(0, time.Since(time.Unix(0, ns)).Seconds())
				s.LagSeconds = &lag
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(w).Encode(struct {
			collector.Status

			Renderer renderer.Snapshot `json:"renderer"`
		}{s, r.Snapshot()}); err != nil {
			return
		}
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

// Originless requests remain available to non-browser WebSocket clients. A
// supplied Origin must match the configured public WebSocket endpoint's
// scheme, host, and effective port; request Host is deliberately irrelevant.
func trustedOrigin(req *http.Request, websocketURL string) bool {
	origins := req.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	configured, ok := parseOrigin(websocketURL, false)
	if !ok {
		return false
	}
	switch configured.scheme {
	case "ws":
		configured.scheme = "http"
	case "wss":
		configured.scheme = "https"
	}
	got, ok := parseOrigin(origins[0], true)
	return ok && got == configured
}

type origin struct{ scheme, host, port string }

func parseOrigin(raw string, bare bool) (origin, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.User != nil || u.Host == "" || u.RawQuery != "" ||
		u.Fragment != "" ||
		u.ForceQuery ||
		(bare && (u.Path != "" || u.RawPath != "")) ||
		strings.ContainsAny(raw, "\r\n\t ,") {
		return origin{}, false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" && scheme != "ws" && scheme != "wss" {
		return origin{}, false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return origin{}, false
	}
	port := u.Port()
	if strings.HasSuffix(u.Host, ":") {
		return origin{}, false
	}
	if port == "" {
		if scheme == "https" || scheme == "wss" {
			port = "443"
		} else {
			port = "80"
		}
	} else {
		for _, c := range port {
			if c < '0' || c > '9' {
				return origin{}, false
			}
		}
		n, e := strconv.Atoi(port)
		if e != nil || n < 1 || n > 65535 {
			return origin{}, false
		}
		port = strconv.Itoa(n)
	}
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	return origin{scheme: scheme, host: host, port: port}, true
}

func delta(current, previous uint64) int {
	d := current - previous
	if d > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(d)
}

func configStale(c *collector.Collector) float64 {
	d, err := time.ParseDuration(c.Config.Collection.StaleAfter)
	if err != nil {
		return 0
	}
	return d.Seconds()
}

func serveReport(w http.ResponseWriter, r *http.Request, path string) {
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "report_unavailable", http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = f.Close() }()
	serveReportFile(w, r, f)
}

func serveReportFile(w http.ResponseWriter, r *http.Request, f interface {
	io.ReadSeeker
	Stat() (os.FileInfo, error)
},
) {
	if _, err := f.Stat(); err != nil {
		http.Error(w, "report_unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	// Last-Modified has second precision, so it cannot distinguish snapshots
	// published within the same second. Serve these no-store reports without it.
	http.ServeContent(w, r, "index.html", time.Time{}, f)
}
