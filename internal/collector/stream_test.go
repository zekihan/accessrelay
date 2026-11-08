package collector

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zekihan/accessrelay/internal/renderer"
)

// feedConsumer records what a streamed replay looked like when GoAccess would
// have started on it.
type feedConsumer struct {
	consumer
	runtime   string
	feedFail  bool
	feeds     int
	feed      *renderer.Feed
	seenBytes int64
}

func (r *feedConsumer) StartFeed(_ context.Context, g string, feed *renderer.Feed) error {
	r.feeds++
	if r.feedFail {
		return errors.New("renderer_feed")
	}
	info, err := os.Stat(filepath.Join(r.runtime, "generations", g, "access.log"))
	if err != nil {
		return err
	}
	r.seenBytes = info.Size()
	r.feed = feed
	r.generation = g
	r.ready = true
	return nil
}

func streamFixture(t *testing.T, stream bool) (*fixture, *feedConsumer) {
	t.Helper()
	previous := replayFeedMargin
	replayFeedMargin = int64(len(line) + 1)
	t.Cleanup(func() { replayFeedMargin = previous })
	f := newFixture(t)
	r := &feedConsumer{runtime: f.config.RuntimeDir}
	f.c.Consumer = r
	f.c.Config.Report.StreamReplay = stream
	rows := make([]map[string]string, 0, 3000)
	for i := range 3000 {
		rows = append(rows, f.row("2026-10-01T00:01:00.123456789Z", strings.Repeat("p", i%7+1)+string(rune('a'+i%26))+string(rune('a'+i/26%26))+string(rune('a'+i/676))))
	}
	f.rows = rows
	f.window(t)
	return f, r
}

func TestStreamedReplayStartsConsumerBeforeExportCompletes(t *testing.T) {
	f, r := streamFixture(t, true)
	if r.feeds != 1 || r.starts != 0 || r.feed == nil {
		t.Fatalf("feeds=%d starts=%d", r.feeds, r.starts)
	}
	replay := f.replay(t)
	if r.seenBytes < 2*replayFeedMargin || r.seenBytes >= int64(len(replay)) {
		t.Fatalf("consumer started at %d of %d bytes", r.seenBytes, len(replay))
	}
	if r.feed.Written() != int64(len(replay)) {
		t.Fatalf("feed counted %d of %d bytes", r.feed.Written(), len(replay))
	}
	if f.c.Generation != r.generation {
		t.Fatal("streamed generation was not adopted")
	}
	// The renderer may only finish its initial parse after Close.
	r.ready = false
	r.generation = ""
	f.c.dirty = true
	if err := f.c.Export(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.feeds != 2 {
		t.Fatalf("feeds=%d", r.feeds)
	}
}

func TestStreamedReplayFallsBackWhenFeedCannotStart(t *testing.T) {
	previous := replayFeedMargin
	replayFeedMargin = int64(len(line) + 1)
	t.Cleanup(func() { replayFeedMargin = previous })
	f := newFixture(t)
	r := &feedConsumer{runtime: f.config.RuntimeDir, feedFail: true}
	f.c.Consumer = r
	f.c.Config.Report.StreamReplay = true
	f.rows = []map[string]string{f.row("2026-10-01T00:01:00.123456789Z", "a"), f.row("2026-10-01T00:01:00.123456789Z", "b"), f.row("2026-10-01T00:01:00.123456789Z", "c")}
	f.window(t)
	if r.feeds != 1 || r.starts != 1 || f.c.Generation == "" {
		t.Fatalf("feeds=%d starts=%d generation=%q", r.feeds, r.starts, f.c.Generation)
	}
}

func TestStreamedReplayDisabledUsesCompleteReplay(t *testing.T) {
	_, r := streamFixture(t, false)
	if r.feeds != 0 || r.starts != 1 {
		t.Fatalf("feeds=%d starts=%d", r.feeds, r.starts)
	}
}

func TestStreamedReplayFailureReleasesGeneration(t *testing.T) {
	f, r := streamFixture(t, true)
	size := int64(len(f.replay(t)))
	stops := r.stops
	// A replay limit just below the export size fails after the consumer started.
	f.c.Config.Limits.MaxReplayBytes = size - 1
	f.c.dirty = true
	if err := f.c.Export(context.Background()); err == nil || err.Error() != "replay_limit" {
		t.Fatalf("err=%v", err)
	}
	if r.feeds != 2 || r.stops != stops+2 || r.generation != "" {
		t.Fatalf("feeds=%d stops=%d generation=%q", r.feeds, r.stops-stops, r.generation)
	}
	entries, err := os.ReadDir(filepath.Join(f.config.RuntimeDir, "generations"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("generations=%v err=%v", entries, err)
	}
}
