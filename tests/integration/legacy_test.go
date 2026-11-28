//go:build integration

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/collector"
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
	now := time.Date(2026, time.October, 1, 0, 1, 0, 123456789, time.UTC)
	line := `192.0.2.1 - - [01/Oct/2026:00:01:00 +0000] "GET /café?q=<x> HTTP/2.0" 200 42 "-" "Miniflux Client Library 😀" 1 "router" "http://192.0.2.2:80" 2ms`
	rows := make([]map[string]string, 0, 9)
	for i := range 4 {
		row := map[string]string{
			"_time":                now.Format(time.RFC3339Nano),
			"_stream_id":           "ストリーム",
			"kubernetes.pod_id":    "pod-a",
			"kubernetes.docker_id": "container-a",
			"_msg":                 line,
		}
		if i == 3 {
			row["kubernetes.pod_id"] = "pod-b"
		}
		rows = append(rows, row)
	}
	for _, space := range []string{"\v", "\u001c", "\u0085", "\u00a0", "\u2028"} {
		rows = append(rows, map[string]string{
			"_time": now.Format(
				time.RFC3339Nano,
			),
			"_stream_id": "fixture",
			"_msg":       strings.Replace(line, " - - [", " - team"+space+"user [", 1),
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for _, row := range rows {
			_ = json.NewEncoder(w).Encode(row)
		}
	}))
	defer server.Close()
	c := config.Default()
	c.Database.MmapBytes = 64 << 20
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "connection.json")
	c.Collection.Source.Cluster = "fixture"
	_ = os.Mkdir(c.StateDir, 0o700)
	_ = os.Mkdir(c.RuntimeDir, 0o700)
	raw, _ := json.Marshal(map[string]string{"url": server.URL})
	_ = os.WriteFile(c.ConnectionFile, raw, 0o600)
	script := `import sys, importlib.util, json
s=importlib.util.spec_from_file_location('legacy',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
settings=json.loads(sys.argv[5]);client=m.Client(settings,sys.argv[3]);db=m.Store(sys.argv[2]+'/events.sqlite');stage=client.fetch(int(sys.argv[6]),int(sys.argv[7]));db.commit_window(stage,int(sys.argv[7]));db.close()
`
	settings, _ := json.Marshal(
		map[string]any{
			"source":         c.Collection.Source,
			"connectionFile": c.ConnectionFile,
			"queryTimeout":   "20s",
			"maxLineBytes":   1048576,
		},
	)
	start := now.Add(-time.Minute).UnixNano()
	end := now.Add(time.Minute).UnixNano()
	args := []string{
		"-c",
		script,
		legacy,
		c.StateDir,
		c.RuntimeDir,
		c.ConnectionFile,
		string(settings),
		jsonNumber(start),
		jsonNumber(end),
	}
	cmd := exec.Command("python3", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	coll, err := collector.New(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := coll.Store
	defer s.Close()
	var mmapBytes int64
	if err := s.DB.QueryRow(`PRAGMA mmap_size`).
		Scan(&mmapBytes); err != nil ||
		mmapBytes != c.Database.MmapBytes {
		t.Fatalf("mapped reads not configured: mmap_size=%d err=%v", mmapBytes, err)
	}
	var bound config.Source
	if err := s.Get("source_identity", &bound); err != nil || bound != c.Collection.Source {
		t.Fatalf("legacy source adoption failed: %v", err)
	}
	client := source.New(c)
	defer client.Close()
	result, err := client.Fetch(t.Context(), start, end)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(result.Path)
	if err = s.CommitWindow(
		t.Context(),
		result.Path,
		end,
		true,
		float64(time.Now().Unix()),
	); err != nil {
		t.Fatal(err)
	}
	a, b, err := s.Counts()
	if err != nil || a != 2 || b != 4 {
		t.Fatalf(
			"after Go collection: events=%d copies=%d err=%v, want 2 events and 4 copies",
			a,
			b,
			err,
		)
	}
	// The original implementation can reopen the same store and query it after accessrelay stops.
	_ = s.Close()
	cmd = exec.Command("python3", "-c", `import sqlite3,sys
c=sqlite3.connect(sys.argv[1]);assert c.execute('select sum(occurrences) from events').fetchone()[0]==4;assert c.execute('pragma user_version').fetchone()[0]==0`, s.Path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(output))
	}
	t.Run("migrate copy to both inputs", func(t *testing.T) {
		testMultiSourceMigration(t, c, s.Path, rows, start, end)
	})
}

func testMultiSourceMigration(
	t *testing.T,
	c config.Config,
	original string,
	rows []map[string]string,
	start, end int64,
) {
	t.Helper()
	c.StateDir = t.TempDir()
	c.RuntimeDir = t.TempDir()
	backup, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.StateDir, "events.sqlite"), backup, 0o600); err != nil {
		t.Fatal(err)
	}
	gate := config.Source{
		Cluster:   c.Collection.Source.Cluster,
		Namespace: "gatehouse",
		Container: "gatehouse",
	}
	c.Collection.Sources = []config.Input{
		{Source: gate, Format: config.GatehouseJSON},
		{Source: c.Collection.Source, Format: config.TraefikCLF},
	}
	c.Collection.ExcludeRoutes = []string{"logs/journald"}

	showSelector := func(row map[string]string, selector config.Source) map[string]string {
		result := map[string]string{
			"cluster": selector.Cluster, "kubernetes.namespace_name": selector.Namespace,
			"kubernetes.container_name": selector.Container,
		}
		maps.Copy(result, row)
		return result
	}
	mixed := make([]map[string]string, 0, len(rows)+3)
	for _, row := range rows {
		mixed = append(mixed, showSelector(row, c.Collection.Source))
	}
	newLog := `{"time":"2026-10-01T00:01:00.123456789Z","request_id":"fixture-1","listener":8443,"client_ip":"192.0.2.3","method":"GET","path":"/gatehouse","proto":"HTTP/2.0","status":200,"bytes_out":12,"duration_ms":1.5,"route":"app/web","backend":"app/web:80"}`
	entry := showSelector(rows[0], gate)
	entry["_msg"] = newLog
	excluded := showSelector(rows[0], gate)
	excluded["_msg"] = strings.Replace(newLog, "app/web", "logs/journald", 1)
	mixed = append(mixed, entry, entry, excluded)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.FormValue("query")
		if !strings.Contains(query, `container_name:="gatehouse") OR (cluster:`) ||
			!strings.Contains(query, `container_name:="traefik")) | fields`) {
			t.Errorf("source selectors missing from query %q", query)
		}
		for _, row := range mixed {
			if err := json.NewEncoder(w).Encode(row); err != nil {
				t.Error(err)
				break
			}
		}
	}))
	defer backend.Close()
	c.ConnectionFile = filepath.Join(t.TempDir(), "connection.json")
	raw, _ := json.Marshal(map[string]string{"url": backend.URL})
	if err := os.WriteFile(c.ConnectionFile, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	client := source.New(c)
	defer client.Close()
	opened, err := collector.New(c, client, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := opened.Store
	defer s.Close()
	var cursor int64
	if err := s.Get("cursor", &cursor); err != nil || cursor != end {
		t.Fatalf("migrated cursor=%d, want %d, err=%v", cursor, end, err)
	}
	checkCounts := func(events, copies int64) {
		t.Helper()
		a, b, err := s.Counts()
		if err != nil || a != events || b != copies {
			t.Fatalf("events=%d copies=%d err=%v, want %d/%d", a, b, err, events, copies)
		}
	}
	checkCounts(2, 4)
	for range 2 {
		result, err := client.Fetch(t.Context(), start, end+1)
		if err != nil {
			t.Fatal(err)
		}
		if result.Excluded != 1 {
			t.Fatalf("excluded=%d, want 1", result.Excluded)
		}
		if err := s.CommitWindow(t.Context(), result.Path, end+1, true, 1); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(result.Path); err != nil {
			t.Fatal(err)
		}
		checkCounts(3, 6)
	}
	var replay bytes.Buffer
	if _, err := s.Export(t.Context(), &replay, 0, true, 1<<20, nil); err != nil {
		t.Fatal(err)
	}
	if strings.Count(replay.String(), rows[0]["_msg"]) != 4 ||
		strings.Count(replay.String(), "GET /gatehouse HTTP/2.0") != 2 {
		t.Fatalf("replay lost old or new occurrences: %s", &replay)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c.Collection.Sources = c.Collection.Sources[:1]
	retired, err := collector.New(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := retired.Store.Close(); err != nil {
		t.Fatal(err)
	}
	changed := c.Collection.Sources[0]
	changed.Namespace = "different"
	c.Collection.Sources = []config.Input{changed}
	if _, err := collector.New(c, nil, nil); err == nil || err.Error() != "source_mismatch" {
		t.Fatalf("changed binding error=%v, want source_mismatch", err)
	}
	c.Collection.Sources = nil
	rolledBack, err := collector.New(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rolledBack.Store.Close()
	if err := rolledBack.Store.Get("cursor", &cursor); err != nil || cursor != end+1 {
		t.Fatalf("rollback cursor=%d, want %d, err=%v", cursor, end+1, err)
	}
	if err := rolledBack.Store.Verify(t.Context()); err != nil {
		t.Fatal(err)
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
	if err := os.Mkdir(state, 0o700); err != nil {
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
	if _, err = s.Export(t.Context(), f, 0, true, 1073741824, nil); err != nil {
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
