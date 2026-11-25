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
		if entry["phase"] != "replay_export" ||
			entry["msg"] != []string{"Phase_start", "Phase_progress", "Phase_finish"}[i] {
			t.Fatalf("unexpected entry: %v", entry)
		}
		if i > 0 {
			elapsed, ok := entry["elapsedSeconds"].(float64)
			if !ok || elapsed < 0 {
				t.Fatal("negative elapsed time")
			}
		}
		if i == 1 && entry["completed"] != float64(2000) {
			t.Fatalf("unexpected progress: %v", entry)
		}
		if i == 2 && (entry["completed"] != float64(3000) || entry["success"] != false) {
			t.Fatalf("unexpected outcome: %v", entry)
		}
	}
}

func TestPhaseProgressOnlyReportsAdvancingWork(t *testing.T) {
	var output bytes.Buffer
	p := start("replay_export", slog.New(slog.NewJSONHandler(&output, nil)))

	p.lastReport = time.Now().Add(-31 * time.Second)
	p.Progress(0)
	if strings.Contains(output.String(), "Phase_progress") {
		t.Errorf("reported progress before any work: %s", output.String())
	}

	output.Reset()
	p.lastReport = time.Now().Add(-31 * time.Second)
	p.Progress(1000)
	if !strings.Contains(output.String(), "Phase_progress") {
		t.Fatal("missing progress after completing work")
	}

	output.Reset()
	p.lastReport = time.Now().Add(-31 * time.Second)
	p.Progress(1000)
	if output.Len() != 0 {
		t.Errorf("reported the same count again: %s", output.String())
	}
	p.Progress(2000)
	if !strings.Contains(output.String(), `"completed":2000`) {
		t.Error("missing progress after the count advanced")
	}
}

func TestDisabledPhase(t *testing.T) {
	var phase *Phase
	phase.Progress(1000)
	phase.Finish(true)
}
