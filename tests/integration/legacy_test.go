//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
	"github.com/zekihan/accessrelay/internal/source"
	"github.com/zekihan/accessrelay/internal/store"
)

func TestLegacyDatabaseDigestAndRollbackCompatibility(t *testing.T) {
	root := t.TempDir()
	legacy, err := filepath.Abs("../legacy/collector.py")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 1, 0, 1, 0, 123456789, time.UTC)
	line := `192.0.2.1 - - [01/Oct/2026:00:01:00 +0000] "GET /café?q=<x> HTTP/2.0" 200 42 "-" "Miniflux Client Library 😀" 1 "router" "http://192.0.2.2:80" 2ms`
	rows := []map[string]string{}
	for i := range 4 {
		row := map[string]string{"_time": now.Format(time.RFC3339Nano), "_stream_id": "ストリーム", "kubernetes.pod_id": "pod-a", "kubernetes.docker_id": "container-a", "_msg": line}
		if i == 3 {
			row["kubernetes.pod_id"] = "pod-b"
		}
		rows = append(rows, row)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for _, row := range rows {
			_ = json.NewEncoder(w).Encode(row)
		}
	}))
	defer server.Close()
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "connection.json")
	c.Collection.Source.Cluster = "fixture"
	_ = os.Mkdir(c.StateDir, 0700)
	_ = os.Mkdir(c.RuntimeDir, 0700)
	raw, _ := json.Marshal(map[string]string{"url": server.URL})
	_ = os.WriteFile(c.ConnectionFile, raw, 0600)
	script := `import sys, importlib.util, json
s=importlib.util.spec_from_file_location('legacy',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
settings=json.loads(sys.argv[5]);client=m.Client(settings,sys.argv[3]);db=m.Store(sys.argv[2]+'/events.sqlite');stage=client.fetch(int(sys.argv[6]),int(sys.argv[7]));db.commit_window(stage,int(sys.argv[7]));db.close()
`
	settings, _ := json.Marshal(map[string]any{"source": c.Collection.Source, "connectionFile": c.ConnectionFile, "queryTimeout": "20s", "maxLineBytes": 1048576})
	start := now.Add(-time.Minute).UnixNano()
	end := now.Add(time.Minute).UnixNano()
	args := []string{"-c", script, legacy, c.StateDir, c.RuntimeDir, c.ConnectionFile, string(settings), jsonNumber(start), jsonNumber(end)}
	cmd := exec.Command("python3", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	s, err := store.Open(c.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := source.New(c)
	defer client.Close()
	result, err := client.Fetch(context.Background(), start, end)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(result.Path)
	if err = s.CommitWindow(context.Background(), result.Path, end, true, float64(time.Now().Unix())); err != nil {
		t.Fatal(err)
	}
	a, b, err := s.Counts()
	if err != nil || a != 2 || b != 4 {
		t.Fatal("legacy digests changed or multiplicity doubled", a, b, err)
	}
	// The original implementation can reopen the same store and query it after accessrelay stops.
	_ = s.Close()
	cmd = exec.Command("python3", "-c", `import sqlite3,sys
c=sqlite3.connect(sys.argv[1]);assert c.execute('select sum(occurrences) from events').fetchone()[0]==4;assert c.execute('pragma user_version').fetchone()[0]==0`, s.Path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
}
func jsonNumber(n int64) string { b, _ := json.Marshal(n); return string(b) }

// TestRetainedDatabaseCopy is opt-in: its input must be an off-volume SQLite backup.
// Both readers operate on new copies; the supplied backup is never opened for writing.
func TestRetainedDatabaseCopy(t *testing.T) {
	backup := os.Getenv("ACCESSRELAY_DATABASE_FIXTURE")
	if backup == "" {
		t.Skip("no isolated retained-database fixture supplied")
	}
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	input, err := os.Open(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.Create(filepath.Join(state, "events.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(output, input); err != nil {
		t.Fatal(err)
	}
	if err = output.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	replay := filepath.Join(root, "go.log")
	f, err := os.Create(replay)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Export(f, 0, true, 1073741824); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	a, b, err := s.Counts()
	if err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(root, "python.log")
	cmd := exec.Command("python3", "-c", `import sqlite3,sys
c=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True)
with open(sys.argv[2],'w') as f:
 for message,copies in c.execute('SELECT message,occurrences FROM events ORDER BY time_ns,id'):
  for _ in range(copies): f.write(message+'\n')
`, s.Path, legacy)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	hash := func(path string) string {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		h := sha256.New()
		if _, err = io.Copy(h, file); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(h.Sum(nil))
	}
	if hash(replay) != hash(legacy) {
		t.Fatal("retained database replay differs from legacy")
	}
	t.Logf("verified %d event identities, %d copies, identical legacy/Go replay", a, b)
}
