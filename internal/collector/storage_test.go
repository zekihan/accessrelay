package collector

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWindowHonorsSymlinkedRuntimeLimit(t *testing.T) {
	f := newFixture(t)
	runtime := f.config.RuntimeDir
	target := runtime + "-target"
	if err := os.Rename(runtime, target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(target), runtime); err != nil {
		t.Fatal(err)
	}
	const limit = 2 << 20
	f.c.Config.Limits.MaxRuntimeBytes = limit
	f.c.Config.Limits.MaxReplayBytes = limit / 2
	used := filepath.Join(target, "cache")
	if err := os.WriteFile(used, make([]byte, limit), 0600); err != nil {
		t.Fatal(err)
	}
	f.rows = []map[string]string{f.row("2026-10-01T00:01:00Z", "a")}

	start := HistoryStart(f.now, f.config.Report.HistoryDays)
	if err := f.c.Window(t.Context(), start, start+int64(5*time.Minute), true); err == nil || err.Error() != "runtime_limit" {
		t.Fatalf("window at runtime limit = %v, want runtime_limit", err)
	}
	if queries := f.c.Snapshot().Queries; queries != 0 {
		t.Fatalf("queried the backend %d times at runtime limit", queries)
	}
	var cursor int64
	if err := f.c.Store.Get("cursor", &cursor); err != nil || cursor != 0 {
		t.Fatalf("cursor = %d, %v; want unset", cursor, err)
	}

	// A link inside runtime does not count the data it points to elsewhere.
	outside := filepath.Join(filepath.Dir(target), "external-cache")
	if err := os.Rename(used, outside); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, used); err != nil {
		t.Fatal(err)
	}
	f.window(t)
	f.counts(t, 1, 1)
}
