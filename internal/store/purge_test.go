package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestPurgeEventsRemovesMatchesWithChangesAndRecordsMarker(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// More rows than one scan batch, with matches spread across batches.
	total := 2*purgeScanBatch + 17
	if _, err = s.DB.Exec(`INSERT INTO sources(id,identity) VALUES(1,'kept'),(2,'purged');
		WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
		INSERT INTO events(digest,time_ns,source_id,message,occurrences)
		SELECT CAST(x AS BLOB),x,CASE WHEN x%3=0 THEN 2 ELSE 1 END,CASE WHEN x%3=0 THEN 'drop' ELSE 'keep' END,2 FROM n;
		INSERT INTO changes(event_id,copies) SELECT id,occurrences FROM events`, total); err != nil {
		t.Fatal(err)
	}
	progress := 0
	removed, err := s.PurgeEvents(context.Background(), func(m string) bool { return strings.HasPrefix(m, "drop") }, "excluded_routers", []string{"r"}, func() { progress++ })
	if err != nil || removed != int64(total/3) || progress == 0 {
		t.Fatalf("removed=%d progress=%d err=%v", removed, progress, err)
	}
	var events, changes, dropped, sources int
	if err = s.DB.QueryRow(`SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM changes),(SELECT COUNT(*) FROM events WHERE message='drop'),(SELECT COUNT(*) FROM sources)`).Scan(&events, &changes, &dropped, &sources); err != nil {
		t.Fatal(err)
	}
	if events != total-total/3 || changes != events || dropped != 0 || sources != 1 {
		t.Fatalf("events=%d changes=%d dropped=%d sources=%d", events, changes, dropped, sources)
	}
	var marker []string
	if err = s.Get("excluded_routers", &marker); err != nil || len(marker) != 1 || marker[0] != "r" {
		t.Fatalf("marker=%v err=%v", marker, err)
	}
	var pages int
	if err = s.DB.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&pages); err != nil || pages != 1000 {
		t.Fatalf("wal_autocheckpoint=%d err=%v", pages, err)
	}
}

func TestPurgeEventsCancellationLeavesMarkerUnset(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.DB.Exec(`INSERT INTO sources(id,identity) VALUES(1,'a');
		INSERT INTO events(digest,time_ns,source_id,message,occurrences) VALUES(x'01',1,1,'drop',1)`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.PurgeEvents(ctx, func(string) bool { return true }, "excluded_routers", []string{"r"}, nil); err == nil {
		t.Fatal("canceled purge succeeded")
	}
	var marker []string
	if err = s.Get("excluded_routers", &marker); err != nil || marker != nil {
		t.Fatalf("marker=%v err=%v", marker, err)
	}
}
