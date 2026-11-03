package service

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

type activeHandler struct{ handler http.Handler }

type StartupHandler struct {
	started time.Time
	active  atomic.Pointer[activeHandler]
}

func NewStartupHandler() *StartupHandler {
	return &StartupHandler{started: time.Now()}
}

func (s *StartupHandler) Activate(handler http.Handler) {
	s.active.Store(&activeHandler{handler: handler})
}

func (s *StartupHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if active := s.active.Load(); active != nil {
		active.handler.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == "/status.json" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"initialized": false, "stale": true, "error": nil,
			"phase": "initializing", "elapsedSeconds": time.Since(s.started).Seconds(),
			"heartbeat": float64(time.Now().UnixNano()) / 1e9,
			"renderer":  map[string]any{"running": false, "ready": false, "restarts": 0},
		})
		return
	}
	// Keep the startup probe active until database validation has completed.
	http.Error(w, "initializing", http.StatusServiceUnavailable)
}
