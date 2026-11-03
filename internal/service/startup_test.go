package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestStartupStatusAndActivation(t *testing.T) {
	handler := NewStartupHandler()
	for _, path := range []string{"/ready", "/health", "/", "/ws", "/metrics"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s became available during initialization", path)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/status.json", nil))
	var status struct {
		Initialized, Stale bool
		Phase              string
		ElapsedSeconds     float64
		Renderer           struct{ Ready bool }
	}
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || response.Header().Get("Cache-Control") != "no-store" || status.Initialized || status.Renderer.Ready || !status.Stale || status.Phase != "initializing" || status.ElapsedSeconds < 0 {
		t.Fatalf("invalid startup status: %s", response.Body)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest("GET", "/ready", nil))
				if response.Code != 503 && response.Code != 204 {
					t.Errorf("unexpected response: %d", response.Code)
				}
			}
		})
	}
	handler.Activate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	wg.Wait()
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/ready", nil))
	if response.Code != 204 {
		t.Fatal("active handler was not installed")
	}
}
