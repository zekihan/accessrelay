package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	commonsmetrics "github.com/zekihan/go-commons/metrics"
)

// TestMiddlewareRecordsMuxPattern serves a request through the production
// chain (registry.Middleware around the startup handler, as in
// cmd/accessrelay/main.go) and checks the recorded route is the mux pattern,
// not "unknown".
func TestMiddlewareRecordsMuxPattern(t *testing.T) {
	c, r := serviceFixture(t)

	startup := NewStartupHandler()
	registry := commonsmetrics.New()
	chain := registry.Middleware(startup)
	startup.Activate(Handler(c, r))

	response := httptest.NewRecorder()
	chain.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/health", http.NoBody),
	)

	if response.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", response.Code)
	}

	recorder := httptest.NewRecorder()
	registry.Handler().ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody),
	)

	body := recorder.Body.String()

	if !strings.Contains(
		body,
		`http_requests_total{method="GET",route="/health",status="200"}`,
	) {
		t.Fatalf("metrics exposition missing mux pattern route:\n%s", body)
	}

	if strings.Contains(body, `route="unknown"`) {
		t.Fatalf("metrics exposition labels a request unknown:\n%s", body)
	}
}
