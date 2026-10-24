package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func openStage(t *testing.T, path string, rows ...stageRow) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE records(digest BLOB PRIMARY KEY,time_ns INTEGER,source TEXT,message TEXT,copies INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if _, err := db.Exec(`INSERT INTO records VALUES(?,?,?,?,?)`, row.digest, row.timeNS, row.source, row.message, row.copies); err != nil {
			t.Fatal(err)
		}
	}
}

type stageRow struct {
	digest  []byte
	timeNS  int64
	source  string
	message string
	copies  int64
}

func TestCommitWindowInitialChangesUsesDigestLookup(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB.Exec(`INSERT INTO sources(identity) VALUES('fixture'); INSERT INTO events(digest,time_ns,source_id,message,occurrences) VALUES(x'01',1,1,'existing',1)`); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(dir, "stage.sqlite")
	openStage(t, stage, stageRow{digest: []byte{1}, timeNS: 1, source: "fixture", message: "existing", copies: 1})
	if _, err := s.DB.Exec(`ATTACH DATABASE ? AS window`, stage); err != nil {
		t.Fatal(err)
	}
	rows, err := s.DB.Query(`EXPLAIN QUERY PLAN ` + insertInitialChanges)
	if err != nil {
		t.Fatal(err)
	}
	var details []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(`DETACH DATABASE window`); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(details, "\n")
	if strings.Contains(strings.ToUpper(plan), "SCAN E") {
		t.Fatalf("initial change plan scans all events: %s", plan)
	}
	if !strings.Contains(plan, "SEARCH e USING INDEX") {
		t.Fatalf("initial change plan lacks an indexed event digest lookup: %s", plan)
	}
}

func TestCommitWindowPreservesMultiplicityIdempotenceAndCursor(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const priorGaps = `[{"start":"retained-start","end":"retained-end"}]`
	if _, err := s.DB.Exec(`INSERT INTO metadata(key,value) VALUES('gaps',?)`, priorGaps); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(dir, "first.sqlite")
	openStage(t, first,
		stageRow{digest: []byte{1}, timeNS: 10, source: "fixture", message: "one", copies: 3},
		stageRow{digest: []byte{2}, timeNS: 11, source: "fixture", message: "two", copies: 1},
	)
	if err := s.CommitWindow(context.Background(), first, 100, true, 1.0); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "second.sqlite")
	openStage(t, second,
		stageRow{digest: []byte{1}, timeNS: 10, source: "fixture", message: "one", copies: 5},
		stageRow{digest: []byte{2}, timeNS: 11, source: "fixture", message: "two", copies: 1},
	)
	if err := s.CommitWindow(context.Background(), second, 90, true, 2.0); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitWindow(context.Background(), second, 90, true, 3.0); err != nil {
		t.Fatal(err)
	}
	var eventCount, copyCount, changeCount int
	if err := s.DB.QueryRow(`SELECT COUNT(*),SUM(occurrences) FROM events`).Scan(&eventCount, &copyCount); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow(`SELECT COUNT(*) FROM changes`).Scan(&changeCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 2 || copyCount != 6 || changeCount != 3 {
		t.Fatalf("events=%d copies=%d changes=%d", eventCount, copyCount, changeCount)
	}
	var cursor int64
	if err := s.Get("cursor", &cursor); err != nil {
		t.Fatal(err)
	}
	if cursor != 100 {
		t.Fatalf("cursor regressed or changed unexpectedly: %d", cursor)
	}
	var success float64
	if err := s.Get("last_success", &success); err != nil {
		t.Fatal(err)
	}
	if success != 3.0 {
		t.Fatalf("last_success=%v", success)
	}
	var sources int
	if err := s.DB.QueryRow(`SELECT COUNT(*) FROM sources`).Scan(&sources); err != nil {
		t.Fatal(err)
	}
	if sources != 1 {
		t.Fatalf("duplicate source rows: %d", sources)
	}
	var gaps string
	if err := s.DB.QueryRow(`SELECT value FROM metadata WHERE key='gaps'`).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	if gaps != priorGaps {
		t.Fatalf("CommitWindow changed gap metadata: %q", gaps)
	}
	var changeCopies string
	rows, err := s.DB.Query(`SELECT copies FROM changes ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var copies int64
		if err := rows.Scan(&copies); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprint(copies))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	changeCopies = strings.Join(got, ",")
	if changeCopies != "3,1,2" {
		t.Fatalf("change multiplicities=%s", changeCopies)
	}
}
