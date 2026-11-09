package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestReplacementConnectionPreservesRetentionCascades(t *testing.T) {
	for _, mmapBytes := range []int64{0, 64 << 20} {
		t.Run(fmt.Sprintf("mmap=%d", mmapBytes), func(t *testing.T) {
			testReplacementConnection(t, mmapBytes)
		})
	}
}

func testReplacementConnection(t *testing.T, mmapBytes int64) {
	t.Helper()
	s, err := OpenWithOptions(filepath.Join(t.TempDir(), "state"), Options{MmapBytes: mmapBytes})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.DB.Exec(`INSERT INTO sources VALUES(1,'fixture');
 INSERT INTO events VALUES(1,x'01',1,1,'expired',2);
 INSERT INTO changes(event_id,copies) VALUES(1,2)`); err != nil {
		t.Fatal(err)
	}
	// Model database/sql discarding a failed physical connection. The SQLite
	// driver also discards interrupted file-backed connections after cancellation.
	conn, err := s.DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
		t.Fatal(err)
	}
	_ = conn.Close()
	if removed, err := s.Prune(context.Background(), 2, nil); err != nil || removed != 1 {
		t.Fatalf("removed=%d err=%v", removed, err)
	}
	var changes int
	if err := s.DB.QueryRow(`SELECT COUNT(*) FROM changes`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes != 0 {
		t.Fatalf("replacement connection left %d orphaned changes after retention", changes)
	}
	for pragma, want := range map[string]int64{"foreign_keys": 1, "synchronous": 2, "busy_timeout": 5000, "journal_size_limit": 8388608, "mmap_size": mmapBytes} {
		var got int64
		if err := s.DB.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s=%d, want %d", pragma, got, want)
		}
	}
}

func TestSynchronousSurvivesReplacementConnection(t *testing.T) {
	for _, tc := range []struct {
		setting string
		want    int64
	}{{"", 2}, {"full", 2}, {"normal", 1}} {
		t.Run(tc.setting, func(t *testing.T) {
			s, err := OpenWithOptions(filepath.Join(t.TempDir(), "state"), Options{Synchronous: tc.setting})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			conn, err := s.DB.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			_ = conn.Close()
			var mode string
			var got int64
			if err := s.DB.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
				t.Fatalf("journal_mode=%q err=%v", mode, err)
			}
			if err := s.DB.QueryRow(`PRAGMA synchronous`).Scan(&got); err != nil || got != tc.want {
				t.Fatalf("synchronous=%d err=%v, want %d", got, err, tc.want)
			}
		})
	}
}

func TestInvalidSynchronousDoesNotCreateState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s, err := OpenWithOptions(path, Options{Synchronous: "off"})
	if s != nil {
		s.Close()
		t.Fatal("invalid synchronous opened a store")
	}
	if err == nil || err.Error() != "configuration_database" {
		t.Fatalf("got %v, want configuration_database", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid synchronous created state: %v", err)
	}
}

func TestInvalidMappingDoesNotCreateState(t *testing.T) {
	for _, mmapBytes := range []int64{-1, 2<<30 + 1} {
		path := filepath.Join(t.TempDir(), "state")
		s, err := OpenWithOptions(path, Options{MmapBytes: mmapBytes})
		if s != nil {
			s.Close()
			t.Fatal("invalid mapping opened a store")
		}
		if err == nil || err.Error() != "configuration_database" {
			t.Fatalf("got %v, want configuration_database", err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid mapping created state: %v", err)
		}
	}
}
