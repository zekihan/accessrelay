package renderer

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
)

func bareRenderer(t *testing.T, mutate func(*config.Config)) *Renderer {
	t.Helper()
	root := t.TempDir()
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	if mutate != nil {
		mutate(&c)
	}
	r, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestNeedsReplayReflectsBackoffAndRunningChild(t *testing.T) {
	r := bareRenderer(
		t,
		func(c *config.Config) { c.GoAccessBinary = filepath.Join(t.TempDir(), "missing") },
	)
	if !r.NeedsReplay() {
		t.Fatal("idle renderer with no retry deadline should need a replay")
	}
	if err := r.Start(t.Context(), "gen"); err == nil || err.Error() != "renderer_start" {
		t.Fatalf("Start err=%v, want renderer_start", err)
	}
	if r.NeedsReplay() {
		t.Fatal("renderer in backoff must not need a replay yet")
	}
	if snapshot := r.Snapshot(); snapshot.Restarts != 1 || snapshot.Error != "renderer_start" {
		t.Fatalf("snapshot=%+v, want one restart with renderer_start", snapshot)
	}
	r.retry = time.Now().Add(-time.Second)
	if !r.NeedsReplay() {
		t.Fatal("renderer past its retry deadline should need a replay")
	}
	running := newTestRenderer(t)
	if running.NeedsReplay() {
		t.Fatal("running renderer must not need a replay")
	}
}

func TestNewImportsOnlyCompleteLegacyReport(t *testing.T) {
	for _, tc := range []struct {
		name    string
		legacy  string
		wantPub bool
	}{
		{"complete", "<html><body>legacy</body></html>", true},
		{"incomplete", "<html><body>legacy", false},
		{"oversize", strings.Repeat("x", 2048) + "</html>", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			c := config.Default()
			c.StateDir = filepath.Join(root, "state")
			c.RuntimeDir = filepath.Join(root, "runtime")
			c.Limits.MaxReportBytes = 1024
			legacy := filepath.Join(c.StateDir, "www/current/index.html")
			if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(legacy, []byte(tc.legacy), 0o600); err != nil {
				t.Fatal(err)
			}
			r, err := New(c)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(r.ReportPath())
			if tc.wantPub {
				if err != nil || !strings.Contains(string(data), "legacy") ||
					!strings.Contains(string(data), "accessrelay-status") {
					t.Fatalf("published=%q err=%v, want annotated legacy report", data, err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatalf("published=%q err=%v, want no report", data, err)
			}
		})
	}
}

func TestNewKeepsExistingReportAndRejectsUnusableStorage(t *testing.T) {
	r := bareRenderer(t, nil)
	if err := os.WriteFile(r.ReportPath(), []byte("<html>current</html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	again, err := New(r.Config)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(
		again.ReportPath(),
	); err != nil ||
		string(data) != "<html>current</html>" {
		t.Fatalf("report=%q err=%v, want untouched current report", data, err)
	}

	root := t.TempDir()
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	if err := os.MkdirAll(c.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.StateDir, "www"), []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c); err == nil || err.Error() != "renderer_storage" {
		t.Fatalf("err=%v, want renderer_storage", err)
	}
}

func TestCompleteFileBoundaries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")
	for _, tc := range []struct {
		name string
		data string
		ok   bool
	}{
		{"complete", "<html><body>x</body></html>", true},
		{"trailing whitespace", "<html></html> \n", true},
		{"empty", "", false},
		{"no closing html", "<html><body>x</body>", false},
		{"closing not at end", "</html> trailing", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.data), 0o600); err != nil {
				t.Fatal(err)
			}
			data, err := completeFile(path, 1024)
			if tc.ok && (err != nil || string(data) != tc.data) {
				t.Fatalf("data=%q err=%v", data, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("data=%q accepted, want rejection", data)
			}
		})
	}
	// Exactly at the limit passes; one byte over fails.
	exact := "<html>" + strings.Repeat("x", 1024-len("<html></html>")) + "</html>"
	if err := os.WriteFile(path, []byte(exact), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := completeFile(path, 1024); err != nil {
		t.Fatalf("at-limit report rejected: %v", err)
	}
	if err := os.WriteFile(path, []byte(exact+"x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := completeFile(path, 1024); err == nil {
		t.Fatal("over-limit report accepted")
	}
	if _, err := completeFile(filepath.Join(dir, "absent"), 1024); err == nil {
		t.Fatal("missing report accepted")
	}
}

func TestAnnotateInsertsBannerOnce(t *testing.T) {
	data := annotate([]byte("<html><body>hi</body></html>"))
	if strings.Count(string(data), `id="accessrelay-status"`) != 1 ||
		!strings.Contains(string(data), "hi</aside>") &&
			!strings.Contains(string(data), "</aside>") {
		t.Fatalf("annotated=%s", data)
	}
	if again := annotate(data); string(again) != string(data) {
		t.Fatal("annotate is not idempotent")
	}
	if got := annotate([]byte("<html>no body</html>")); string(got) != "<html>no body</html>" {
		t.Fatalf("report without </body> changed: %s", got)
	}
	multi := annotate([]byte("<body>a</body><body>b</body>"))
	if strings.Count(string(multi), "</body>") != 2 ||
		strings.Index(
			string(multi),
			"accessrelay-status",
		) > strings.Index(
			string(multi),
			"b</body>",
		) {
		t.Fatalf("banner not inserted at first </body>: %s", multi)
	}
}

func TestNormalizedOverrideSpellings(t *testing.T) {
	for _, tc := range []struct{ key, in, want string }{
		{"http-method", "true", "yes"},
		{"http-method", "false", "no"},
		{"http-protocol", "yes", "yes"},
		{"http-protocol", "no", "no"},
		{"anonymize-ip", "yes", "true"},
		{"anonymize-ip", "no", "false"},
		{"anonymize-ip", "true", "true"},
		{"ignore-crawlers", "false", "false"},
	} {
		if got := normalizedOverride(tc.key, tc.in); got != tc.want {
			t.Errorf("normalizedOverride(%q,%q)=%q, want %q", tc.key, tc.in, got, tc.want)
		}
	}
}

func TestAtomicFileReplacesAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out")
	if err := atomicFile(path, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(path, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "two" {
		t.Fatalf("data=%q err=%v", data, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v, want only the published file", entries, err)
	}
	if err := atomicFile(filepath.Join(dir, "missing", "out"), []byte("x")); err == nil {
		t.Fatal("atomicFile into a missing directory succeeded")
	}
}

func TestReadDiagnosticTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	content := "0123456789abcdefghijklmnopqrstuvwxyz"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		limit int64
		want  string
	}{
		{100, content},
		{int64(len(content)), content},
		{10, content[len(content)-10:]},
		{1, "z"},
	} {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := readDiagnosticTail(f, tc.limit)
		_ = f.Close()
		if err != nil || string(got) != tc.want {
			t.Errorf("limit=%d got=%q err=%v, want %q", tc.limit, got, err, tc.want)
		}
	}
}

// TestDiagnosticRingMatchesTailModel appends deterministic chunks of varying
// sizes and compares the flushed snapshot against a naive last-N-bytes model.
func TestDiagnosticRingMatchesTailModel(t *testing.T) {
	for _, limit := range []int64{1, 7, 16, 64} {
		t.Run(strconv.FormatInt(limit, 10), func(t *testing.T) {
			snapshot, err := os.CreateTemp(t.TempDir(), "snapshot")
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			c := &diagnosticCapture{snapshot: snapshot, limit: limit, buffer: make([]byte, limit)}
			var model []byte
			seed := uint32(12345)
			next := func() byte { seed = seed*1664525 + 1013904223; return byte(seed >> 24) }
			for chunk := range 200 {
				n := int(next()) % int(3*limit+2)
				data := make([]byte, n)
				for i := range data {
					data[i] = next()
				}
				c.append(data)
				model = append(model, data...)
				if len(model) > int(limit) {
					model = model[len(model)-int(limit):]
				}
				c.flush()
				if c.writeError.Load() {
					t.Fatal("flush reported a write error")
				}
				got, err := os.ReadFile(snapshot.Name())
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != string(model) {
					t.Fatalf("chunk %d (n=%d): snapshot %q, want tail %q", chunk, n, got, model)
				}
			}
		})
	}
}

func TestStartDiagnosticCaptureRejectsBadLimitAndCarriesSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "unknowns.txt")
	if err := ensureDiagnosticFIFO(path); err != nil {
		t.Fatal(err)
	}
	if _, err := startDiagnosticCapture(path, 0); err == nil || err.Error() != "diagnostic_limit" {
		t.Fatalf("err=%v, want diagnostic_limit", err)
	}
	if err := os.WriteFile(path+".snapshot", []byte("previous"), 0o600); err != nil {
		t.Fatal(err)
	}
	capture, err := startDiagnosticCapture(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteString("+new"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	capture.close()
	if capture.writeError.Load() {
		t.Fatal("capture reported a write error")
	}
	data, err := os.ReadFile(path + ".snapshot")
	if err != nil || string(data) != "previous+new" {
		t.Fatalf("snapshot=%q err=%v, want carried previous+new", data, err)
	}
	// Closing twice is safe.
	capture.close()
}

func TestEnsureDiagnosticFIFOShapes(t *testing.T) {
	dir := t.TempDir()
	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureDiagnosticFIFO(regular); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(regular); err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("regular file not replaced by FIFO: %v", err)
	}
	if err := ensureDiagnosticFIFO(regular); err != nil {
		t.Fatalf("existing FIFO rejected: %v", err)
	}
	asDir := filepath.Join(dir, "dir")
	if err := os.Mkdir(asDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ensureDiagnosticFIFO(asDir); err == nil || err.Error() != "diagnostic_path" {
		t.Fatalf("err=%v, want diagnostic_path", err)
	}
}

func TestFeedAdvanceAndCloseAreIdempotent(t *testing.T) {
	feed := NewFeed(10)
	if feed.Written() != 0 {
		t.Fatal("new feed already has bytes")
	}
	if _, ok := feed.closed(); ok {
		t.Fatal("new feed reports closed")
	}
	feed.Advance(5)
	feed.Advance(7)
	if feed.Written() != 12 {
		t.Fatalf("Written=%d, want 12", feed.Written())
	}
	feed.Close()
	at, ok := feed.closed()
	if !ok || at.IsZero() {
		t.Fatalf("closed=(%v,%t)", at, ok)
	}
	feed.Close()
	if again, _ := feed.closed(); !again.Equal(at) {
		t.Fatalf("second Close moved the close time: %v -> %v", at, again)
	}
	r := bareRenderer(t, nil)
	if started, complete := r.feedClosed(); !complete || !started.IsZero() {
		t.Fatalf("feedClosed without feed=(%v,%t), want zero,true", started, complete)
	}
	r.feed = feed
	if _, complete := r.feedClosed(); !complete {
		t.Fatal("feedClosed with closed feed reports incomplete")
	}
	r.feed = NewFeed(1)
	if _, complete := r.feedClosed(); complete {
		t.Fatal("feedClosed with open feed reports complete")
	}
}

func TestTickMarksReadyWhenWebSocketReachable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:7890")
	if err != nil {
		t.Skipf("port 7890 unavailable: %v", err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	root := t.TempDir()
	binary := filepath.Join(root, "goaccess")
	script := "#!/bin/sh\ntrap 'exit 0' INT TERM\nwhile :; do sleep .1; done\n"
	// #nosec G306 -- test executables need the execute bit.
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
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
	if err := r.Start(t.Context(), "ready-generation"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(t.Context()) })
	if err := os.WriteFile(r.output, []byte("<html><body>done</body></html>"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Stale derived directories are cleaned once the first report is ready.
	stale := filepath.Join(c.StateDir, "db", "stale")
	if err := os.MkdirAll(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	r.tick(t.Context())
	snapshot := r.Snapshot()
	if !snapshot.Ready || !snapshot.Running || snapshot.Error != "" {
		t.Fatalf("snapshot=%+v, want ready and running", snapshot)
	}
	data, err := os.ReadFile(r.ReportPath())
	if err != nil || !strings.Contains(string(data), "done") ||
		!strings.Contains(string(data), "accessrelay-status") {
		t.Fatalf("report=%q err=%v", data, err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale derived directory survived readiness: %v", err)
	}
	// A second tick leaves a ready renderer alone.
	r.tick(t.Context())
	if snapshot := r.Snapshot(); !snapshot.Ready {
		t.Fatalf("ready renderer lost readiness: %+v", snapshot)
	}
}

func TestTickSurfacesDiagnosticWriteErrorWithoutChild(t *testing.T) {
	r := bareRenderer(t, nil)
	capture := &diagnosticCapture{}
	capture.writeError.Store(true)
	r.diagnostics = []*diagnosticCapture{capture}
	r.tick(t.Context())
	if snapshot := r.Snapshot(); snapshot.Error != "renderer_diagnostic_storage" {
		t.Fatalf("snapshot=%+v, want renderer_diagnostic_storage", snapshot)
	}
	r.diagnostics = nil
	r.status.Error = ""
	r.tick(t.Context())
	if snapshot := r.Snapshot(); snapshot.Error != "" {
		t.Fatalf("idle tick changed status: %+v", snapshot)
	}
}

func TestDiagnosticPathShapesBlockStart(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		r := bareRenderer(t, nil)
		target := filepath.Join(t.TempDir(), "target.txt")
		if err := os.WriteFile(target, []byte("diag"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, r.diagnosticPaths()[0]); err != nil {
			t.Fatal(err)
		}
		if err := r.prepareDiagnosticPath(
			r.diagnosticPaths()[0],
		); err == nil ||
			err.Error() != "diagnostic_path" {
			t.Fatalf("err=%v, want diagnostic_path", err)
		}
		if err := r.Stop(t.Context()); err == nil || err.Error() != "renderer_diagnostic_storage" {
			t.Fatalf("Stop err=%v, want renderer_diagnostic_storage", err)
		}
		// The link target must never be written through the diagnostic path.
		if data, err := os.ReadFile(target); err != nil || string(data) != "diag" {
			t.Fatalf("target=%q err=%v, want untouched", data, err)
		}
	})
	t.Run("unreadable regular file", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores file permissions")
		}
		r := bareRenderer(t, nil)
		path := r.diagnosticPaths()[0]
		if err := os.WriteFile(path, []byte("diag"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
		if err := r.prepareDiagnosticPath(path); err == nil {
			t.Fatal("unreadable diagnostic file accepted")
		}
	})
}

func TestStartFeedCanceledContext(t *testing.T) {
	r := bareRenderer(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.StartFeed(ctx, "feed-gen", NewFeed(1)); err == nil {
		t.Fatal("StartFeed with canceled context succeeded")
	}
	if got := r.Generation(); got != "" || r.feed != nil {
		t.Fatalf("canceled StartFeed left generation %q / feed %v", got, r.feed)
	}
}
