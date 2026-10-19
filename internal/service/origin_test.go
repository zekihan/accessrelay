package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zekihan/accessrelay/internal/config"
	"github.com/zekihan/accessrelay/internal/renderer"
)

func TestTrustedOrigin(t *testing.T) {
	const configured = "wss://Report.Example:443/analytics/ws"
	for _, tc := range []struct {
		name   string
		origin string
		want   bool
	}{
		{"configured origin", "https://report.example", true},
		{"configured explicit port", "https://REPORT.EXAMPLE:443", true},
		{"foreign host", "https://foreign.example", false},
		{"foreign port", "https://report.example:444", false},
		{"wrong scheme", "http://report.example:443", false},
		{"null", "null", false},
		{"empty", "", false},
		{"path", "https://report.example/path", false},
		{"malformed port", "https://report.example:bad", false},
		{"credentials", "https://user@report.example", false},
		{"query", "https://report.example?x=1", false},
		{"fragment", "https://report.example#fragment", false},
		{"websocket scheme", "wss://report.example", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://internal.invalid/ws", nil)
			req.Host = "attacker-controlled.invalid"
			req.Header.Set("Origin", tc.origin)
			if got := trustedOrigin(req, configured); got != tc.want {
				t.Fatalf("trustedOrigin(%q) = %v, want %v", tc.origin, got, tc.want)
			}
		})
	}
	t.Run("originless client", func(t *testing.T) {
		req := httptest.NewRequest("GET", "http://internal.invalid/ws", nil)
		if !trustedOrigin(req, configured) {
			t.Fatal("rejected originless client")
		}
	})
	t.Run("multiple origins", func(t *testing.T) {
		req := httptest.NewRequest("GET", "http://internal.invalid/ws", nil)
		req.Header.Add("Origin", "https://report.example")
		req.Header.Add("Origin", "https://foreign.example")
		if trustedOrigin(req, configured) {
			t.Fatal("accepted multiple origins")
		}
	})
	t.Run("IPv6 and default ports", func(t *testing.T) {
		req := httptest.NewRequest("GET", "http://internal.invalid/ws", nil)
		req.Header.Set("Origin", "https://[2001:db8::1]")
		if !trustedOrigin(req, "wss://[2001:db8:0::1]:443/custom/path") {
			t.Fatal("rejected equivalent IPv6 address and default port")
		}
	})
	t.Run("handler rejects before renderer readiness", func(t *testing.T) {
		cfg := config.Default()
		r := &renderer.Renderer{Config: cfg}
		handler := Handler(nil, r)
		req := httptest.NewRequest(http.MethodGet, "/ws", nil)
		req.Header.Set("Origin", "https://foreign.example")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusForbidden {
			t.Fatalf("foreign Origin status = %d, want %d", response.Code, http.StatusForbidden)
		}
	})
	t.Run("handler allows configured origin to reach readiness", func(t *testing.T) {
		cfg := config.Default()
		r := &renderer.Renderer{Config: cfg}
		handler := Handler(nil, r)
		req := httptest.NewRequest(http.MethodGet, "/ws", nil)
		req.Header.Set("Origin", "http://localhost:8080")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("configured Origin status = %d, want readiness %d", response.Code, http.StatusServiceUnavailable)
		}
	})
}
