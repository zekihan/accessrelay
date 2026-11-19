package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWaitForShutdownReportsHTTPTimeoutAfterCollectorExit(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		<-release
	}))
	defer server.Close()
	defer close(release)

	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	ctx, cancel := context.WithDeadline(t.Context(), time.Now())
	defer cancel()
	if err := waitForShutdown(ctx, server.Config, nil); err == nil || err.Error() != "shutdown_timeout" {
		t.Fatalf("got %v, want shutdown_timeout with an active request", err)
	}
}
