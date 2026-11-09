package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// corruptIndex invalidates the events_time index root page while leaving the
// schema readable, so only quick_check detects the damage.
func corruptIndex(t *testing.T, dir string) {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB.Exec(`INSERT INTO sources VALUES(1,'fixture');
 INSERT INTO events VALUES(1,x'01',1,1,'message',1)`); err != nil {
		t.Fatal(err)
	}
	var root, pageSize int64
	if err := s.DB.QueryRow(`SELECT rootpage FROM sqlite_master WHERE name='events_time'`).Scan(&root); err != nil {
		t.Fatal(err)
	}
	if err := s.DB.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.sqlite"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, (root-1)*pageSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDeferredIntegrityCheck(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	corruptIndex(t, dir)
	if s, err := Open(dir); err == nil || err.Error() != "database_integrity" {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("startup check: got %v, want database_integrity", err)
	}
	s, err := OpenWithOptions(dir, Options{DeferIntegrity: true})
	if err != nil {
		t.Fatal("deferred open:", err)
	}
	defer s.Close()
	if err := s.Verify(context.Background()); err == nil || err.Error() != "database_integrity" {
		t.Fatalf("verify: got %v, want database_integrity", err)
	}
}

func TestVerifyRunsAlongsideWriter(t *testing.T) {
	s, err := OpenWithOptions(filepath.Join(t.TempDir(), "state"), Options{MmapBytes: 64 << 20, DeferIntegrity: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tx, err := s.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO sources VALUES(1,'fixture')`); err != nil {
		t.Fatal(err)
	}
	// An open write transaction must not block or fail the check.
	if err := s.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Verify(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled verify: got %v", err)
	}
	if _, err := s.DB.Exec(`INSERT INTO metadata VALUES('k','1')`); err != nil {
		t.Fatal("writer after verify:", err)
	}
}
