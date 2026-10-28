package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func retentionFixture(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	_, err = s.DB.Exec(`INSERT INTO metadata VALUES('cursor','1000');
		INSERT INTO sources(id,identity) VALUES(1,'expired'),(2,'retained'),(3,'unused');
		WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
		INSERT INTO events(digest,time_ns,source_id,message,occurrences)
		SELECT CAST(x AS BLOB),1,1,'fixture',2 FROM n;
		INSERT INTO events(digest,time_ns,source_id,message,occurrences) VALUES(x'ff',100,2,'retained',3);
		INSERT INTO changes(event_id,copies) SELECT id,occurrences FROM events`, 2*pruneBatchSize+17)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPruneBatchesPreserveCutoffCascadesAndMetadata(t *testing.T) {
	s := retentionFixture(t)
	callbacks := 0
	lastCount := 2*pruneBatchSize + 18
	removed, err := s.Prune(context.Background(), 100, func() {
		callbacks++
		var remaining int
		if err := s.DB.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if delta := lastCount - remaining; delta <= 0 || delta > pruneBatchSize {
			t.Fatalf("unexpected committed batch size: %d", delta)
		}
		lastCount = remaining
	})
	if err != nil || removed != 2*pruneBatchSize+17 || callbacks != 3 {
		t.Fatalf("removed=%d callbacks=%d err=%v", removed, callbacks, err)
	}
	var events, changes, sources, copies int
	if err := s.DB.QueryRow(`SELECT (SELECT COUNT(*) FROM events), (SELECT COUNT(*) FROM changes),
		(SELECT COUNT(*) FROM sources), (SELECT SUM(occurrences) FROM events)`).Scan(&events, &changes, &sources, &copies); err != nil {
		t.Fatal(err)
	}
	if events != 1 || changes != 1 || sources != 1 || copies != 3 {
		t.Fatalf("events=%d changes=%d sources=%d copies=%d", events, changes, sources, copies)
	}
	var cursor int
	if err := s.Get("cursor", &cursor); err != nil || cursor != 1000 {
		t.Fatalf("cursor=%d err=%v", cursor, err)
	}
	removed, err = s.Prune(context.Background(), 100, nil)
	if err != nil || removed != 0 {
		t.Fatalf("repeat removed=%d err=%v", removed, err)
	}
}

func TestPruneCancellationRetainsCommittedProgressAndResumes(t *testing.T) {
	s := retentionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	removed, err := s.Prune(ctx, 100, cancel)
	if !errors.Is(err, context.Canceled) || removed != pruneBatchSize {
		t.Fatalf("removed=%d err=%v", removed, err)
	}
	remaining, err := s.Prune(context.Background(), 100, nil)
	if err != nil || remaining != pruneBatchSize+17 {
		t.Fatalf("remaining=%d err=%v", remaining, err)
	}
}

func TestPruneSelectsExpiredBatchThroughTimeIndex(t *testing.T) {
	s := retentionFixture(t)
	rows, err := s.DB.Query(`EXPLAIN QUERY PLAN DELETE FROM events WHERE id IN (
		SELECT id FROM events WHERE time_ns<? ORDER BY time_ns,id LIMIT ?
	)`, 100, pruneBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(plan, "\n"), "USING COVERING INDEX events_time") {
		t.Fatalf("retention batch lacks indexed cutoff lookup: %v", plan)
	}
}

type cancelReplay struct{ cancel context.CancelFunc }

func (w cancelReplay) Write(b []byte) (int, error) {
	w.cancel()
	return len(b), nil
}

func TestExportCancellationLeavesAuthoritativeStateUnchanged(t *testing.T) {
	s := retentionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := s.Export(ctx, cancelReplay{cancel}, 0, true, 1<<20, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("export ignored cancellation: %v", err)
	}
	events, copies, err := s.Counts()
	if err != nil || events != 2*pruneBatchSize+18 || copies != 2*(2*pruneBatchSize+17)+3 {
		t.Fatalf("events=%d copies=%d err=%v", events, copies, err)
	}
}
