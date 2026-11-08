package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplayPrefetchDoesNotChangeOutput(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO sources VALUES(1,'fixture')`); err != nil {
		t.Fatal(err)
	}
	var want strings.Builder
	type event struct{ time, copies int64 }
	events := map[int64]event{}
	// Insert rows out of time order so the time index and the table disagree.
	for id := int64(1); id <= 3000; id++ {
		e := event{time: 1_000_000_000 + (id*7919)%3000*1_000_003, copies: 1 + id%3}
		events[id] = e
		if _, err = tx.Exec(`INSERT INTO events VALUES(?,?,?,1,?,?)`, id, fmt.Appendf(nil, "%d", id), e.time, fmt.Sprintf("message %d %s", id, strings.Repeat("x", int(id%200))), e.copies); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := s.DB.Query(`SELECT message,occurrences FROM events ORDER BY time_ns,id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var message string
		var copies int
		if err = rows.Scan(&message, &copies); err != nil {
			t.Fatal(err)
		}
		want.WriteString(strings.Repeat(message+"\n", copies))
	}
	_ = rows.Close()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, readers := range []int{0, 1, 4, 16} {
		t.Run(fmt.Sprint(readers), func(t *testing.T) {
			s, err := OpenWithOptions(dir, Options{ReplayPrefetch: readers})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			var output bytes.Buffer
			if _, err := s.Export(t.Context(), &output, 0, true, 1<<30, nil); err != nil {
				t.Fatal(err)
			}
			if output.String() != want.String() {
				t.Fatalf("output differs with %d prefetch readers", readers)
			}
		})
	}
}

func TestReplayPrefetchOptionBounds(t *testing.T) {
	for _, readers := range []int{-1, 65} {
		if _, err := OpenWithOptions(filepath.Join(t.TempDir(), "state"), Options{ReplayPrefetch: readers}); err == nil || err.Error() != "configuration_database" {
			t.Fatalf("readers=%d err=%v", readers, err)
		}
	}
}
