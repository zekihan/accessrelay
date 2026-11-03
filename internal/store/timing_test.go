package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestStoragePhaseDiagnostics(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(previous)
	s := replayFixture(t)
	if _, err := s.Prune(t.Context(), 1000000001, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Export(t.Context(), io.Discard, 0, true, 1<<20, nil); err != nil {
		t.Fatal(err)
	}
	before := output.Len()
	if _, err := s.Export(t.Context(), io.Discard, 0, false, 1<<20, nil); err != nil {
		t.Fatal(err)
	}
	if output.Len() != before {
		t.Fatal("incremental export emitted rebuild logs")
	}
	finished := map[string]float64{}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["msg"] == "phase_finish" {
			if entry["success"] != true || entry["elapsed_seconds"].(float64) < 0 {
				t.Fatalf("unexpected outcome: %v", entry)
			}
			finished[entry["phase"].(string)] = entry["completed"].(float64)
		}
	}
	for _, name := range []string{"database_open", "database_integrity", "retention", "retention_delete", "source_cleanup", "retention_checkpoint", "replay_export"} {
		if _, ok := finished[name]; !ok {
			t.Fatalf("missing phase %q", name)
		}
	}
	if finished["retention_delete"] != 1 || finished["replay_export"] != 3 {
		t.Fatalf("unexpected counts: %v", finished)
	}
	for _, private := range []string{s.Path, "earlier", "later", "fixture"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("diagnostics exposed %q", private)
		}
	}

	output.Reset()
	if _, err := s.Export(t.Context(), failedReplayWriter{}, 0, true, 1<<20, nil); err == nil {
		t.Fatal("expected flush failure")
	}
	if strings.Contains(output.String(), "private filesystem details") || !strings.Contains(output.String(), `"success":false`) {
		t.Fatalf("unsafe or missing failure diagnostic: %s", output.String())
	}
	output.Reset()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Prune(ctx, 1000000002, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if count := strings.Count(output.String(), `"success":false`); count != 2 {
		t.Fatalf("missing cancellation outcomes: %s", output.String())
	}
}
