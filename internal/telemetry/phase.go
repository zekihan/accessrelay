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
	logger.Info("Phase_start", slog.String("phase", name))
	return p
}

func (p *Phase) Progress(completed int64) {
	if p == nil || p.finished || completed <= p.completed {
		return
	}
	p.completed = completed
	now := time.Now()
	if now.Sub(p.lastReport) >= 30*time.Second {
		p.logger.Info(
			"Phase_progress",
			slog.String("phase", p.name),
			slog.Float64("elapsedSeconds", now.Sub(p.started).Seconds()),
			slog.Int64("completed", completed),
		)
		p.lastReport = now
	}
}

func (p *Phase) Finish(success bool) {
	if p == nil || p.finished {
		return
	}
	p.finished = true
	p.logger.Info(
		"Phase_finish",
		slog.String("phase", p.name),
		slog.Float64("elapsedSeconds", time.Since(p.started).Seconds()),
		slog.Int64("completed", p.completed),
		slog.Bool("success", success),
	)
}
