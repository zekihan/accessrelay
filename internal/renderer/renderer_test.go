package renderer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
)

func newTestRenderer(t *testing.T) *Renderer {
	t.Helper()
	root := t.TempDir()
	binary := filepath.Join(root, "goaccess")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.GoAccessBinary = binary
	r, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(t.Context(), "test-generation"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}

func TestRunReapsExitedChild(t *testing.T) {
	r := newTestRenderer(t)
	child := r.cmd
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go r.Run(ctx)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot := r.Snapshot()
		if snapshot.Error == "renderer_exit" {
			if snapshot.Running || snapshot.Ready || snapshot.Restarts != 1 || r.Generation() != "" {
				t.Fatalf("unexpected exit status: %+v", snapshot)
			}
			if child.ProcessState == nil || !child.ProcessState.Exited() || child.ProcessState.ExitCode() != 7 {
				t.Fatal("failed child was not reaped with its exit status")
			}
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("supervisor did not observe child completion")
		}
	}
}

func TestStopPublishesOnlyCompleteFinalReport(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "incomplete"
		if complete {
			name = "complete"
		}
		t.Run(name, func(t *testing.T) {
			r := newTestRenderer(t)
			previous := "<html><body>previous</body></html>"
			if err := os.WriteFile(r.ReportPath(), []byte(previous), 0600); err != nil {
				t.Fatal(err)
			}
			final := "<html><body>final"
			if complete {
				final += "</body></html>"
			}
			if err := os.WriteFile(r.output, []byte(final), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := r.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(r.ReportPath())
			if err != nil {
				t.Fatal(err)
			}
			if complete {
				if !strings.Contains(string(data), "final") || !strings.Contains(string(data), "accessrelay-status") {
					t.Fatalf("complete final report was not published: %s", data)
				}
			} else if string(data) != previous {
				t.Fatalf("incomplete report replaced the last complete report: %s", data)
			}
			if snapshot := r.Snapshot(); snapshot.Running || snapshot.Ready {
				t.Fatalf("renderer is still active after shutdown: %+v", snapshot)
			}
		})
	}
}
