package renderer

import (
	"sync/atomic"
	"time"
)

// Feed lets GoAccess parse a replay while the collector is still writing it.
// GoAccess ends its initial parse at the first end of file and never rewrites
// that snapshot, so the renderer suspends the child whenever its read offset
// comes within Margin bytes of the written length. The child resumes for good
// only after Close, when the replay is complete and synced.
type Feed struct {
	Margin   int64
	written  atomic.Int64
	closedAt atomic.Int64
}

func NewFeed(margin int64) *Feed { return &Feed{Margin: margin} }

// Advance records bytes that have reached the replay file.
func (f *Feed) Advance(n int64) { f.written.Add(n) }
func (f *Feed) Written() int64  { return f.written.Load() }

// Close marks the replay complete; GoAccess may then read to end of file.
func (f *Feed) Close() { f.closedAt.CompareAndSwap(0, time.Now().UnixNano()) }

func (f *Feed) closed() (time.Time, bool) {
	at := f.closedAt.Load()
	return time.Unix(0, at), at != 0
}

type throttle struct {
	quit chan struct{}
	done chan struct{}
}

func (t *throttle) stop() {
	if t == nil {
		return
	}
	close(t.quit)
	<-t.done
}
