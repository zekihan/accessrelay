package store

import (
	"path/filepath"
	"reflect"
	"testing"
)

func indexNames(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.DB.Query(`SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return names
}

func TestFreshStoreCreatesSourceIndex(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	want := []string{"changes_event", "events_source", "events_time"}
	if got := indexNames(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("indexes=%v, want %v", got, want)
	}
	var column string
	if err := s.DB.QueryRow(`SELECT name FROM pragma_index_info('events_source')`).Scan(&column); err != nil || column != "source_id" {
		t.Fatalf("events_source column=%q err=%v", column, err)
	}
}

func TestSchemaIsIdempotentAndPreservesExistingSourceIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(`INSERT INTO sources VALUES(1,'fixture');
 INSERT INTO events VALUES(1,x'01',1,1,'kept',3);
 INSERT INTO changes(event_id,copies) VALUES(1,3)`); err != nil {
		t.Fatal(err)
	}
	// A store created by the one-off maintenance helper already has this index.
	if _, err := s.DB.Exec(Schema); err != nil {
		t.Fatalf("schema is not idempotent: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := indexNames(t, s); len(got) != 3 {
		t.Fatalf("indexes=%v", got)
	}
	var occurrences int
	if err := s.DB.QueryRow(`SELECT occurrences FROM events WHERE id=1`).Scan(&occurrences); err != nil || occurrences != 3 {
		t.Fatalf("occurrences=%d err=%v", occurrences, err)
	}
}

func TestOpeningExistingStoreDoesNotBuildMissingIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(`DROP INDEX events_source`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Startup holds the writer lock and liveness probes are running; an index
	// build over a large existing table must not happen implicitly.
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, name := range indexNames(t, s) {
		if name == "events_source" {
			t.Fatal("opening an existing store rebuilt events_source")
		}
	}
}
