package collector

import (
	"context"
	"strings"
	"testing"

	"github.com/zekihan/accessrelay/internal/source"
)

func TestExcludedRoutersArePurgedOnceAndNotCollected(t *testing.T) {
	f := newFixture(t)
	other := strings.Replace(line, "router@kubernetes", "other@kubernetes", 1)
	f.rows = []map[string]string{f.row("2026-10-01T00:01:00Z", "pod-a"), f.row("2026-10-01T00:02:00Z", "pod-a")}
	f.rows[1]["_msg"] = other
	f.window(t)
	f.counts(t, 2, 2)
	if err := f.c.Store.Close(); err != nil {
		t.Fatal(err)
	}

	f.config.Collection.ExcludeRouters = []string{"router@kubernetes"}
	f.client = source.New(f.config)
	defer f.client.Close()
	c, err := New(f.config, f.client, f.r)
	if err != nil {
		t.Fatal(err)
	}
	f.c = c
	if _, err = c.Tick(context.Background(), f.now); err != nil {
		t.Fatal(err)
	}
	f.counts(t, 1, 1)
	var applied []string
	if err = c.Store.Get("excluded_routers", &applied); err != nil || len(applied) != 1 || applied[0] != "router@kubernetes" {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	if replay := f.replay(t); strings.Contains(replay, `"router@kubernetes"`) || !strings.Contains(replay, "other@kubernetes") {
		t.Fatalf("replay kept excluded router: %q", replay)
	}
	// Collection skips the excluded router even when its lines are fetched again.
	f.window(t)
	f.counts(t, 1, 1)

	// An unchanged list does not scan again, so a matching row inserted
	// directly survives the next start.
	if _, err = c.Store.DB.Exec(`INSERT INTO events(digest,time_ns,source_id,message,occurrences) SELECT x'aa',time_ns,source_id,?,1 FROM events LIMIT 1`, line); err != nil {
		t.Fatal(err)
	}
	if err = c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = New(f.config, f.client, f.r)
	if err != nil {
		t.Fatal(err)
	}
	f.c = c
	if _, err = c.Tick(context.Background(), f.now); err != nil {
		t.Fatal(err)
	}
	f.counts(t, 2, 2)

	// Removing the exclusion purges nothing; collection fetches the router's
	// lines again where the backend still has them.
	if err = c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	f.config.Collection.ExcludeRouters = nil
	f.client = source.New(f.config)
	defer f.client.Close()
	c, err = New(f.config, f.client, f.r)
	if err != nil {
		t.Fatal(err)
	}
	f.c = c
	if _, err = c.Tick(context.Background(), f.now); err != nil {
		t.Fatal(err)
	}
	f.counts(t, 3, 3)
	applied = []string{"stale"}
	if err = c.Store.Get("excluded_routers", &applied); err != nil || len(applied) != 0 {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
}
