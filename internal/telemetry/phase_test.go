package telemetry

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestPhaseProgressIsBoundedAndFinishIsFinal(t *testing.T) {
	var output bytes.Buffer
	p := start("replay_export", slog.New(slog.NewJSONHandler(&output, nil)))
	p.Progress(1000)
	if lines := strings.Count(output.String(), "\n"); lines != 1 {
		t.Fatalf("early progress logged %d lines", lines)
	}
	p.lastReport = time.Now().Add(-31 * time.Second)
	p.Progress(2000)
	p.Progress(3000)
	p.Finish(false)
	p.Finish(true)
	p.Progress(4000)
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("unexpected logs: %s", output.String())
	}
	for i, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["phase"] != "replay_export" || entry["msg"] != []string{"phase_start", "phase_progress", "phase_finish"}[i] {
			t.Fatalf("unexpected entry: %v", entry)
		}
		if i > 0 && entry["elapsed_seconds"].(float64) < 0 {
			t.Fatal("negative elapsed time")
		}
		if i == 1 && entry["completed"] != float64(2000) {
			t.Fatalf("unexpected progress: %v", entry)
		}
		if i == 2 && (entry["completed"] != float64(3000) || entry["success"] != false) {
			t.Fatalf("unexpected outcome: %v", entry)
		}
	}
}

func TestDisabledPhase(t *testing.T) {
	var phase *Phase
	phase.Progress(1000)
	phase.Finish(true)
}
