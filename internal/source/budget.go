package source

import (
	"context"
	"io"

	"github.com/zekihan/accessrelay/internal/config"
)

// WindowBudget is shared by the entire sequential adaptive split tree. Failed
// requests, discarded stages and duplicate/operational records consume it too.
// It is deliberately not reset when a child commits or a connection is reopened.
type WindowBudget struct {
	queries, maxQueries int
	bytes, maxBytes     int64
	records, maxRecords int64
	exhausted           bool
}

type windowBudgetKey struct{}

func WithWindowBudget(ctx context.Context, c config.Collection) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, config.Duration(c.WindowTimeout))
	b := &WindowBudget{maxQueries: c.MaxWindowQueries, maxBytes: c.MaxWindowTotalBytes, maxRecords: c.MaxWindowRecords}
	return context.WithValue(ctx, windowBudgetKey{}, b), cancel
}

func WindowBudgetFrom(ctx context.Context) *WindowBudget {
	b, _ := ctx.Value(windowBudgetKey{}).(*WindowBudget)
	return b
}

func (b *WindowBudget) Check(ctx context.Context) error {
	if b.exhausted || ctx.Err() != nil {
		return queryError("window_budget", false)
	}
	return nil
}

func (b *WindowBudget) BeginQuery(ctx context.Context) error {
	if b.queries >= b.maxQueries {
		b.exhausted = true
	}
	if err := b.Check(ctx); err != nil {
		return err
	}
	b.queries++
	return nil
}

func (b *WindowBudget) StageRecord(ctx context.Context) error {
	if b.records >= b.maxRecords {
		b.exhausted = true
	}
	if err := b.Check(ctx); err != nil {
		return err
	}
	b.records++
	return nil
}

type budgetReader struct {
	body   io.Reader
	budget *WindowBudget
	ctx    context.Context
}

func (r *budgetReader) Read(p []byte) (int, error) {
	if err := r.budget.Check(r.ctx); err != nil {
		return 0, err
	}
	// One extra byte distinguishes an exact-bound EOF from an oversized body.
	// Charge reads rather than parsed lines, including bufio read-ahead.
	remaining := r.budget.maxBytes - r.budget.bytes
	if int64(len(p)) > remaining+1 {
		p = p[:remaining+1]
	}
	n, err := r.body.Read(p)
	r.budget.bytes += int64(n)
	if r.budget.bytes > r.budget.maxBytes {
		r.budget.exhausted = true
		return n, queryError("window_budget", false)
	}
	return n, err
}
