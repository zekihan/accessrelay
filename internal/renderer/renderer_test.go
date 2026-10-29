package renderer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
			for _, path := range r.diagnosticPaths() {
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() {
					t.Fatalf("clean stop did not restore regular diagnostic path %s: %v", path, err)
				}
			}
		})
	}
}

func TestNewThenStopLeavesRegularDiagnosticFiles(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "goaccess")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
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
	for _, path := range r.diagnosticPaths() {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("New unexpectedly prepared diagnostic path %s: %v", path, err)
		}
	}
	if err := r.Stop(t.Context()); err != nil {
		t.Fatalf("Stop returned an error without a child: %v", err)
	}
	if status := r.Snapshot(); status.Error != "" {
		t.Fatalf("clean stop reported an error: %+v", status)
	}
	for _, path := range r.diagnosticPaths() {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("clean stop did not leave regular diagnostic path %s: %v", path, err)
		}
		data, err := os.ReadFile(path)
		if err != nil || len(data) != 0 {
			t.Fatalf("clean stop did not leave an empty diagnostic file %s: %q (%v)", path, data, err)
		}
	}
}

func TestStopRestoresSnapshotWhenDiagnosticPathIsMissing(t *testing.T) {
	root := t.TempDir()
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	r, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	path := r.diagnosticPaths()[0]
	if err := os.WriteFile(path+".snapshot", []byte("saved diagnostic"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "saved diagnostic" {
		t.Fatalf("Stop did not restore the available snapshot: %q (%v)", data, err)
	}
}

func TestStopReportsDiagnosticRestorationFailure(t *testing.T) {
	root := t.TempDir()
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	r, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(r.diagnosticPaths()[0], 0700); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(t.Context()); err == nil || err.Error() != "renderer_diagnostic_storage" {
		t.Fatalf("Stop error = %v, want renderer_diagnostic_storage", err)
	}
	if status := r.Snapshot(); status.Error != "renderer_diagnostic_storage" {
		t.Fatalf("Stop did not report the opaque storage error: %+v", status)
	}
}

func TestFailedChildStartRestoresDiagnosticsWithoutPriorSnapshot(t *testing.T) {
	root := t.TempDir()
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.GoAccessBinary = filepath.Join(root, "missing-goaccess")
	r, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate FIFO paths left by an interrupted older renderer with no
	// corresponding snapshots. Start must first restore them, then clean up
	// the FIFOs it creates if exec fails.
	for _, path := range r.diagnosticPaths() {
		if err := ensureDiagnosticFIFO(path); err != nil {
			t.Fatal(err)
		}
		_ = os.Remove(path + ".snapshot")
	}
	if err := r.Start(t.Context(), "test-generation"); err == nil || err.Error() != "renderer_start" {
		t.Fatalf("Start error = %v, want renderer_start", err)
	}
	for _, path := range r.diagnosticPaths() {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("failed child start left a FIFO at %s: %v", path, err)
		}
		data, err := os.ReadFile(path)
		if err != nil || len(data) != 0 {
			t.Fatalf("failed child start did not restore an empty file %s: %q (%v)", path, data, err)
		}
	}
}

func TestPartialDiagnosticSetupFailureRestoresCreatedFIFO(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "goaccess")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
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
	paths := r.diagnosticPaths()
	if err := r.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(paths[1]); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(paths[1], 0700); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	err = r.startLocked(t.Context(), "test-generation")
	r.mu.Unlock()
	if err == nil || err.Error() != "renderer_diagnostic_storage" {
		t.Fatalf("Start error = %v, want renderer_diagnostic_storage", err)
	}
	info, err := os.Stat(paths[0])
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("partial startup left the first diagnostic FIFO behind: %v", err)
	}
}

func TestBooleanOverridesUseGoAccessSpellings(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "goaccess")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.GoAccessBinary = binary
	c.Report.ConfigOverrides = map[string]string{"http-method": "true", "http-protocol": "false", "anonymize-ip": "yes", "ignore-crawlers": "no"}
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
	conf := filepath.Join(r.Config.RuntimeDir, "generations", "test-generation", "goaccess.conf")
	data, err := os.ReadFile(conf)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"http-method yes", "http-protocol no", "anonymize-ip true", "ignore-crawlers false"} {
		if !strings.Contains(text, want) {
			t.Errorf("generated config missing %q:\n%s", want, text)
		}
	}
}

func TestDiagnosticFIFOIsDrainedIntoBoundedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknowns.txt")
	if err := ensureDiagnosticFIFO(path); err != nil {
		t.Fatal(err)
	}
	capture, err := startDiagnosticCapture(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("0123456789abcdefghijklmnop")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	capture.close()
	data, err := os.ReadFile(path + ".snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "abcdefghijklmnop" {
		t.Fatalf("snapshot was not bounded: %q", data)
	}
}

func TestUnreadyChildTimesOutAndIsReaped(t *testing.T) {
	for _, report := range []string{"missing", "incomplete", "oversize"} {
		t.Run(report, func(t *testing.T) {
			root := t.TempDir()
			binary := filepath.Join(root, "goaccess")
			if err := os.WriteFile(binary, []byte("#!/bin/sh\ntrap 'exit 0' INT TERM\nwhile :; do sleep .1; done\n"), 0700); err != nil {
				t.Fatal(err)
			}
			c := config.Default()
			c.StateDir = filepath.Join(root, "state")
			c.RuntimeDir = filepath.Join(root, "runtime")
			c.GoAccessBinary = binary
			c.Limits.MaxReportBytes = 1024
			r, err := New(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Start(t.Context(), "test-generation"); err != nil {
				t.Fatal(err)
			}
			switch report {
			case "incomplete":
				if err := os.WriteFile(r.output, []byte("<html><body>partial"), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := os.WriteFile(r.output, []byte(strings.Repeat("x", 1025)), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := r.Stop(ctx); err != nil {
					t.Error(err)
				}
			})
			r.initTimeout = 30 * time.Millisecond
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
				if snapshot.Error == "renderer_unready" {
					if snapshot.Running || snapshot.Ready || r.Generation() != "" {
						t.Fatalf("unexpected timeout status: %+v", snapshot)
					}
					if child.ProcessState == nil {
						t.Fatal("unready renderer was not reaped")
					}
					status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
					if !child.ProcessState.Exited() && (!ok || !status.Signaled()) {
						t.Fatal("unready renderer was not reaped")
					}
					return
				}
				select {
				case <-ticker.C:
				case <-deadline.C:
					t.Fatalf("unready child did not time out: %+v", snapshot)
				}
			}
		})
	}
}

func TestStartCanceledDoesNotChangeGeneration(t *testing.T) {
	for _, running := range []bool{false, true} {
		name := "idle"
		if running {
			name = "existing-generation"
		}
		t.Run(name, func(t *testing.T) {
			r := newTestRenderer(t)
			if !running {
				if err := r.Stop(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			previous := r.Generation()
			child := r.cmd
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := r.Start(ctx, "canceled-generation"); !errors.Is(err, context.Canceled) {
				t.Errorf("Start error = %v, want context.Canceled", err)
			}
			if got := r.Generation(); got != previous || r.cmd != child {
				t.Errorf("canceled Start replaced generation %q with %q", previous, got)
			}
			if _, err := os.Stat(filepath.Join(r.Config.RuntimeDir, "generations", "canceled-generation")); !os.IsNotExist(err) {
				t.Errorf("canceled Start prepared a generation: %v", err)
			}
		})
	}
}
