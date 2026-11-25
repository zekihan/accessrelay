package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPutGetRoundTripAndMetadataErrors(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A missing key leaves the caller's value untouched.
	value := "unchanged"
	if err := s.Get("absent", &value); err != nil || value != "unchanged" {
		t.Fatalf("Get absent: value=%q err=%v", value, err)
	}
	if err := s.Put("greeting", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := s.Get("greeting", &value); err != nil || value != "hello" {
		t.Fatalf("Get: value=%q err=%v", value, err)
	}
	if err := s.Put("greeting", "overwritten"); err != nil {
		t.Fatal(err)
	}
	if err := s.Get("greeting", &value); err != nil || value != "overwritten" {
		t.Fatalf("Get after overwrite: value=%q err=%v", value, err)
	}
	var number int
	if err := s.Put("answer", 42); err != nil {
		t.Fatal(err)
	}
	if err := s.Get("answer", &number); err != nil || number != 42 {
		t.Fatalf("Get number: %d err=%v", number, err)
	}
	// Unmarshalable values and corrupt stored JSON surface opaque codes.
	if err := s.Put("bad", make(chan int)); err == nil || err.Error() != "database_transaction" {
		t.Fatalf("Put unmarshalable: err=%v, want database_transaction", err)
	}
	if _, err := s.DB.Exec(
		`INSERT OR REPLACE INTO metadata VALUES('corrupt','not json')`,
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Get("corrupt", &value); err == nil || err.Error() != "database_metadata" {
		t.Fatalf("Get corrupt: err=%v, want database_metadata", err)
	}
}

func TestGapCapsHistoryAndAdvancesCursor(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 1; i <= 25; i++ {
		if err := s.Gap(
			fmt.Sprintf("start-%d", i),
			fmt.Sprintf("end-%d", i),
			int64(i),
		); err != nil {
			t.Fatal(err)
		}
	}
	var gaps []Gap
	if err := s.Get("gaps", &gaps); err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 20 || gaps[0].Start != "start-6" || gaps[19].End != "end-25" {
		t.Fatalf("gaps=%v, want newest 20 (start-6..end-25)", gaps)
	}
	var cursor int64
	if err := s.Get("cursor", &cursor); err != nil || cursor != 25 {
		t.Fatalf("cursor=%d err=%v, want 25", cursor, err)
	}
	// Corrupt gap history must not be silently discarded.
	if _, err := s.DB.Exec(`INSERT OR REPLACE INTO metadata VALUES('gaps','{')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Gap("x", "y", 99); err == nil || err.Error() != "database_metadata" {
		t.Fatalf("Gap with corrupt history: err=%v, want database_metadata", err)
	}
	if err := s.Get("cursor", &cursor); err != nil || cursor != 25 {
		t.Fatalf("cursor after failed Gap=%d err=%v, want unchanged 25", cursor, err)
	}
}

func TestLatestCountsAndSize(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if latest, err := s.Latest(); err != nil || latest != 0 {
		t.Fatalf("Latest empty=%d err=%v, want 0", latest, err)
	}
	if events, copies, err := s.Counts(); err != nil || events != 0 || copies != 0 {
		t.Fatalf("Counts empty=(%d,%d) err=%v, want (0,0)", events, copies, err)
	}
	if _, err := s.DB.Exec(`INSERT INTO sources VALUES(1,'fixture');
		INSERT INTO events VALUES(1,x'01',100,1,'one',3),(2,x'02',200,1,'two',2);
		INSERT INTO changes(seq,event_id,copies) VALUES(5,1,3),(9,2,2)`); err != nil {
		t.Fatal(err)
	}
	if latest, err := s.Latest(); err != nil || latest != 9 {
		t.Fatalf("Latest=%d err=%v, want 9", latest, err)
	}
	if events, copies, err := s.Counts(); err != nil || events != 2 || copies != 5 {
		t.Fatalf("Counts=(%d,%d) err=%v, want (2,5)", events, copies, err)
	}
	size, err := s.Size()
	if err != nil {
		t.Fatal(err)
	}
	var want int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if info, statErr := os.Stat(s.Path + suffix); statErr == nil {
			want += info.Size()
		}
	}
	if size != want || size <= 0 {
		t.Fatalf("Size=%d, want %d (sum of database files)", size, want)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Missing database files contribute nothing rather than an error.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(s.Path + suffix)
	}
	if size, err := s.Size(); err != nil || size != 0 {
		t.Fatalf("Size with no files=%d err=%v, want 0", size, err)
	}
}

func TestOpenRejectsUnusableState(t *testing.T) {
	t.Run("second writer", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")
		first, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Close()
		second, err := Open(dir)
		if second != nil {
			_ = second.Close()
		}
		if err == nil || err.Error() != "writer_in_use" {
			t.Fatalf("err=%v, want writer_in_use", err)
		}
		// The failed attempt must not release the first writer's lock.
		if err := first.Put("still", "locked"); err != nil {
			t.Fatalf("first writer lost its lock: %v", err)
		}
	})
	t.Run("state path is a file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(
			filepath.Join(file, "state"),
		); err == nil ||
			err.Error() != "state_storage" {
			t.Fatalf("err=%v, want state_storage", err)
		}
	})
	t.Run("unsupported user version", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.Exec(`PRAGMA user_version=1`); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); err == nil || err.Error() != "database_format" {
			t.Fatalf("err=%v, want database_format", err)
		}
	})
	t.Run("extra table", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.Exec(`CREATE TABLE stray(id INTEGER)`); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); err == nil || err.Error() != "database_format" {
			t.Fatalf("err=%v, want database_format", err)
		}
	})
	t.Run("wrong column", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.DB.Exec(
			`DROP TABLE metadata; CREATE TABLE metadata(key TEXT PRIMARY KEY, wrong TEXT NOT NULL)`,
		); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); err == nil || err.Error() != "database_format" {
			t.Fatalf("err=%v, want database_format", err)
		}
	})
}

func TestCommitWindowStageErrors(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// A stage path inside a missing directory cannot be attached.
	missing := filepath.Join(dir, "no-such-dir", "stage.sqlite")
	if err := s.CommitWindow(
		t.Context(),
		missing,
		1,
		true,
		1,
	); err == nil ||
		err.Error() != "database_stage" {
		t.Fatalf("missing stage dir: err=%v, want database_stage", err)
	}
	// A stage database without the records table fails inside the transaction.
	empty := filepath.Join(dir, "empty.sqlite")
	if f, err := os.Create(empty); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitWindow(
		t.Context(),
		empty,
		1,
		true,
		1,
	); err == nil ||
		err.Error() != "database_transaction" {
		t.Fatalf("recordless stage: err=%v, want database_transaction", err)
	}
	// Neither failure may advance the cursor.
	var cursor int64
	if err := s.Get("cursor", &cursor); err != nil || cursor != 0 {
		t.Fatalf("cursor=%d err=%v, want untouched 0", cursor, err)
	}
}

func TestCommitWindowRejectsCorruptCursorMetadata(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB.Exec(`INSERT INTO metadata VALUES('cursor','not-a-number')`); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(dir, "stage.sqlite")
	openStage(
		t,
		stage,
		stageRow{digest: []byte{1}, timeNS: 10, source: "fixture", message: "one", copies: 1},
	)
	if err := s.CommitWindow(
		t.Context(),
		stage,
		100,
		true,
		1,
	); err == nil ||
		err.Error() != "database_metadata" {
		t.Fatalf("err=%v, want database_metadata", err)
	}
	// The whole window rolls back: no event may be committed without its cursor.
	if events, _, err := s.Counts(); err != nil || events != 0 {
		t.Fatalf("events=%d err=%v after rejected window, want 0", events, err)
	}
}

func TestExportRejectsNonPositiveCopies(t *testing.T) {
	s := replayFixture(t)
	if _, err := s.DB.Exec(`INSERT INTO changes(seq,event_id,copies) VALUES(4,1,0)`); err != nil {
		t.Fatal(err)
	}
	var out discardWriter
	if latest, err := s.Export(
		t.Context(),
		out,
		3,
		false,
		1<<20,
		nil,
	); latest != 0 || err == nil ||
		err.Error() != "export_database" {
		t.Fatalf("latest=%d err=%v, want export_database", latest, err)
	}
	if _, err := s.DB.Exec(
		`DELETE FROM changes WHERE seq=4; INSERT INTO changes(seq,event_id,copies) VALUES(4,1,-2)`,
	); err != nil {
		t.Fatal(err)
	}
	if latest, err := s.Export(
		t.Context(),
		out,
		3,
		false,
		1<<20,
		nil,
	); latest != 0 || err == nil ||
		err.Error() != "export_database" {
		t.Fatalf("negative copies: latest=%d err=%v, want export_database", latest, err)
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
