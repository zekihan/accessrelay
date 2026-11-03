// Package telemetry records bounded diagnostics for startup and rebuild work.
package telemetry

import (
	"log/slog"
	"time"
)

// Phase is owned by the operation it measures; callers serialize access.
// Names must be static identifiers, never paths or external input.
type Phase struct {
	name       string
	logger     *slog.Logger
	started    time.Time
	lastReport time.Time
	completed  int64
	finished   bool
}

func Start(name string) *Phase { return start(name, slog.Default()) }

func start(name string, logger *slog.Logger) *Phase {
	now := time.Now()
	p := &Phase{name: name, logger: logger, started: now, lastReport: now}
	logger.Info("phase_start", "phase", name)
	return p
}

func (p *Phase) Progress(completed int64) {
	if p == nil || p.finished {
		return
	}
	p.completed = completed
	now := time.Now()
	if now.Sub(p.lastReport) >= 30*time.Second {
		p.logger.Info("phase_progress", "phase", p.name, "elapsed_seconds", now.Sub(p.started).Seconds(), "completed", completed)
		p.lastReport = now
	}
}

func (p *Phase) Finish(success bool) {
	if p == nil || p.finished {
		return
	}
	p.finished = true
	p.logger.Info("phase_finish", "phase", p.name, "elapsed_seconds", time.Since(p.started).Seconds(), "completed", p.completed, "success", success)
}
