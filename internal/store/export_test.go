package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func replayFixture(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.DB.Exec(`INSERT INTO metadata VALUES('cursor','1000');
		INSERT INTO sources VALUES(1,'fixture');
		INSERT INTO events VALUES(1,x'01',1000000001,1,'later',3),(2,x'02',1000000000,1,'earlier',2);
		INSERT INTO changes(seq,event_id,copies) VALUES(1,1,1),(2,2,2),(3,1,2)`); err != nil {
		t.Fatal(err)
	}
	return s
}

type replayWriter struct {
	bytes.Buffer
	writes int
}

func (w *replayWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.Buffer.Write(p)
}

func TestBufferedReplayPreservesOrderAndMultiplicity(t *testing.T) {
	s := replayFixture(t)
	for _, tc := range []struct {
		name  string
		full  bool
		after int64
		want  string
	}{
		{"full", true, 0, "earlier\nearlier\nlater\nlater\nlater\n"},
		{"incremental", false, 1, "earlier\nearlier\nlater\nlater\n"},
		{"empty", false, 3, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output replayWriter
			latest, err := s.Export(t.Context(), &output, tc.after, tc.full, int64(len(tc.want)), nil)
			if err != nil || latest != 3 || output.String() != tc.want {
				t.Fatalf("latest=%d output=%q err=%v", latest, output.String(), err)
			}
			wantWrites := 1
			if tc.want == "" {
				wantWrites = 0
			}
			if output.writes != wantWrites {
				t.Fatalf("writes=%d, want %d", output.writes, wantWrites)
			}
		})
	}
}

type failedReplayWriter struct{ short bool }

func (w failedReplayWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("private filesystem details")
}

func TestReplayFlushFailureDoesNotReturnSequence(t *testing.T) {
	s := replayFixture(t)
	for _, full := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			latest, err := s.Export(t.Context(), failedReplayWriter{short}, 0, full, 1<<20, nil)
			if latest != 0 || err == nil || err.Error() != "export_write" {
				t.Fatalf("full=%t short=%t latest=%d err=%v", full, short, latest, err)
			}
		}
	}
	var cursor int64
	if err := s.Get("cursor", &cursor); err != nil || cursor != 1000 {
		t.Fatalf("cursor=%d err=%v", cursor, err)
	}
	var output bytes.Buffer
	latest, err := s.Export(t.Context(), &output, 0, true, 1<<20, nil)
	if err != nil || latest != 3 || output.String() != "earlier\nearlier\nlater\nlater\nlater\n" {
		t.Fatalf("retry latest=%d output=%q err=%v", latest, output.String(), err)
	}
}

func TestReplayCrossesBufferBoundaryAndChecksCancellation(t *testing.T) {
	s := replayFixture(t)
	if _, err := s.DB.Exec(`UPDATE events SET occurrences=50000 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	want := "earlier\nearlier\n" + strings.Repeat("later\n", 50000)
	var output replayWriter
	if _, err := s.Export(t.Context(), &output, 0, true, int64(len(want)), nil); err != nil {
		t.Fatal(err)
	}
	if output.String() != want || output.writes != 2 {
		t.Fatalf("bytes=%d writes=%d", output.Len(), output.writes)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	latest, err := s.Export(ctx, cancelReplay{cancel}, 0, true, int64(len(want)), nil)
	if latest != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("latest=%d err=%v", latest, err)
	}
	latest, err = s.Export(t.Context(), io.Discard, 0, true, int64(len(want))-1, nil)
	if latest != 0 || err == nil || err.Error() != "replay_limit" {
		t.Fatalf("limited latest=%d err=%v", latest, err)
	}
}
