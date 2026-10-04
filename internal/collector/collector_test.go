package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
	"github.com/zekihan/accessrelay/internal/renderer"
	"github.com/zekihan/accessrelay/internal/source"
)

const line = `192.0.2.1 - - [01/Oct/2026:00:01:00 +0000] "GET /hello?q=1 HTTP/2.0" 200 42 "-" "Miniflux Client Library" 1 "router@kubernetes" "http://192.0.2.2:80" 2ms`

type consumer struct {
	generation    string
	ready         bool
	starts, stops int
	fail          bool
}

func (r *consumer) Start(_ context.Context, g string) error {
	r.starts++
	if r.fail {
		return errors.New("renderer_start")
	}
	r.generation = g
	r.ready = true
	return nil
}
func (r *consumer) Stop(_ context.Context) error {
	r.stops++
	r.generation = ""
	r.ready = false
	return nil
}
func (r *consumer) NeedsReplay() bool  { return r.generation == "" }
func (r *consumer) Generation() string { return r.generation }
func (r *consumer) Snapshot() renderer.Snapshot {
	return renderer.Snapshot{Ready: r.ready, Running: r.ready}
}

type fixture struct {
	c       *Collector
	r       *consumer
	rows    []map[string]string
	mu      sync.Mutex
	backend *httptest.Server
	client  *source.Client
	config  config.Config
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{r: &consumer{}, now: time.Date(2026, 10, 1, 0, 10, 0, 0, time.UTC)}
	f.backend = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = r.ParseForm()
		start, _ := source.ParseTimestamp(r.Form.Get("start"))
		end, _ := source.ParseTimestamp(r.Form.Get("end"))
		for _, row := range f.rows {
			ns, _ := source.ParseTimestamp(row["_time"])
			if ns >= start && ns < end {
				_ = json.NewEncoder(w).Encode(row)
			}
		}
	}))
	root := t.TempDir()
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "connection.json")
	c.Collection.Source.Cluster = "fixture"
	c.Collection.ReconciliationInterval = "1s"
	c.Report.HistoryDays = 1
	raw, _ := json.Marshal(map[string]string{"url": f.backend.URL})
	_ = os.WriteFile(c.ConnectionFile, raw, 0600)
	f.client = source.New(c)
	f.config = c
	var err error
	f.c, err = New(c, f.client, f.r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.c.Store.Close(); f.client.Close(); f.backend.Close() })
	return f
}
func (f *fixture) row(stamp, pod string) map[string]string {
	return map[string]string{"_time": stamp, "_stream_id": "stream-a", "kubernetes.pod_id": pod, "kubernetes.docker_id": "container-a", "_msg": line}
}
func (f *fixture) window(t *testing.T) {
	t.Helper()
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	if err := f.c.Window(context.Background(), start, start+5*int64(time.Minute), true); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) counts(t *testing.T, events, copies int64) {
	t.Helper()
	a, b, e := f.c.Store.Counts()
	if e != nil || a != events || b != copies {
		t.Fatal(a, b, e)
	}
}
func (f *fixture) replay(t *testing.T) string {
	t.Helper()
	data, e := os.ReadFile(filepath.Join(f.config.RuntimeDir, "generations", f.c.Generation, "access.log"))
	if e != nil {
		t.Fatal(e)
	}
	return string(data)
}
func TestOverlapMultiplicityRestartAndFailedExport(t *testing.T) {
	f := newFixture(t)
	f.rows = []map[string]string{f.row("2026-10-01T00:01:00.123456789Z", "a"), f.row("2026-10-01T00:01:00.123456789Z", "a"), f.row("2026-10-01T00:01:00.123456789Z", "b"), f.row("2026-10-01T00:01:00.123456788Z", "a")}
	f.window(t)
	f.window(t)
	f.counts(t, 3, 4)
	if f.replay(t) != line+"\n"+line+"\n"+line+"\n"+line+"\n" {
		t.Fatal("replay differs")
	}
	f.rows = append(f.rows, f.rows[0])
	f.window(t)
	f.counts(t, 3, 5)
	// Replace the export file with a directory to fail after the authoritative commit.
	path := filepath.Join(f.config.RuntimeDir, "generations", f.c.Generation, "access.log")
	_ = os.Remove(path)
	_ = os.Mkdir(path, 0700)
	f.rows = append(f.rows, f.rows[0])
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	if f.c.Window(context.Background(), start, start+5*int64(time.Minute), true) == nil {
		t.Fatal("export should fail")
	}
	f.counts(t, 3, 6)
	if f.r.ready {
		t.Fatal("consumer not stopped after failed export")
	}
	if err := f.c.Export(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.replay(t)) != 6*(len(line)+1) {
		t.Fatal("failed export counted twice")
	}
	old := f.c.Generation
	_ = f.c.Store.Close()
	var e error
	f.c, e = New(f.config, f.client, f.r)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.c.Export(context.Background()); e != nil {
		t.Fatal(e)
	}
	if f.c.Generation == old || len(f.replay(t)) != 6*(len(line)+1) {
		t.Fatal("restart replay")
	}
	f.window(t)
	f.counts(t, 3, 6)
}
func TestTransactionalRollbackAndWriterExclusion(t *testing.T) {
	f := newFixture(t)
	f.rows = []map[string]string{f.row("2026-10-01T00:01:00Z", "a")}
	if _, e := New(f.config, f.client, &consumer{}); e == nil {
		t.Fatal("competing writer accepted")
	}
	_, e := f.c.Store.DB.Exec(`CREATE TRIGGER fail_cursor BEFORE INSERT ON metadata BEGIN SELECT RAISE(ABORT,'fixture');END`)
	if e != nil {
		t.Fatal(e)
	}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	if f.c.Window(context.Background(), start, start+5*int64(time.Minute), true) == nil {
		t.Fatal("transaction should fail")
	}
	f.counts(t, 0, 0)
	var cursor int64
	_ = f.c.Store.Get("cursor", &cursor)
	if cursor != 0 {
		t.Fatal(cursor)
	}
	_, _ = f.c.Store.DB.Exec(`DROP TRIGGER fail_cursor`)
	f.window(t)
	f.counts(t, 1, 1)
}
func TestRetentionRolloverReconciliationAndGap(t *testing.T) {
	f := newFixture(t)
	f.rows = []map[string]string{f.row("2026-10-01T00:01:00Z", "a")}
	for i := range 12 {
		if _, e := f.c.Tick(context.Background(), f.now.Add(time.Duration(i)*time.Second)); e != nil {
			t.Fatal(e)
		}
	}
	f.counts(t, 1, 1)
	f.rows = append(f.rows, f.row("2026-10-01T00:00:01Z", "late"))
	f.c.timeReconcile = time.Time{}
	for i := range 12 {
		if _, e := f.c.Tick(context.Background(), f.now.Add(time.Duration(i+12)*time.Second)); e != nil {
			t.Fatal(e)
		}
	}
	f.counts(t, 2, 2)
	old := f.c.Generation
	next := f.now.AddDate(0, 0, 1)
	if _, e := f.c.Tick(context.Background(), next); e != nil {
		t.Fatal(e)
	}
	f.counts(t, 0, 0)
	if f.c.Generation == old {
		t.Fatal("rollover did not regenerate")
	}
	var gaps []struct{ Start, End string }
	_ = f.c.Store.Get("gaps", &gaps)
	if len(gaps) != 1 {
		t.Fatal(gaps)
	}
	if e := f.c.Cleanup(); e != nil {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(filepath.Join(f.config.RuntimeDir, "generations"))
	if len(entries) != 1 {
		t.Fatal(entries)
	}
	want := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC).UnixNano()
	if HistoryStart(f.now, 7) != want {
		t.Fatal("calendar retention")
	}
}

type splitter struct {
	Backend
	failSecond bool
	calls      int
}

func (s *splitter) Fetch(ctx context.Context, start, end int64) (source.Result, error) {
	s.calls++
	if end-start > 2*int64(time.Second) {
		return source.Result{}, &source.QueryError{Reason: "timeout", Split: true}
	}
	if s.failSecond && s.calls == 3 {
		return source.Result{}, &source.QueryError{Reason: "backend_request"}
	}
	return s.Backend.Fetch(ctx, start, end)
}
func TestTimeoutSplitDoesNotAdvancePastFailedHalf(t *testing.T) {
	f := newFixture(t)
	f.rows = []map[string]string{f.row("2026-10-01T00:00:01Z", "a")}
	split := &splitter{Backend: f.client, failSecond: true}
	f.c.Backend = split
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	end := start + 4*int64(time.Second)
	if e := f.c.Window(context.Background(), start, end, true); e == nil {
		t.Fatal("second half should fail")
	}
	var cursor int64
	_ = f.c.Store.Get("cursor", &cursor)
	if cursor != start+2*int64(time.Second) {
		t.Fatal(cursor)
	}
	f.counts(t, 1, 1)
	split.failSecond = false
	if e := f.c.Window(context.Background(), start, end, true); e != nil {
		t.Fatal(e)
	}
	f.counts(t, 1, 1)
	_ = f.c.Store.Get("cursor", &cursor)
	if cursor != end {
		t.Fatal(cursor)
	}
}
func TestEmptyBootStorageBoundAndRendererFailure(t *testing.T) {
	f := newFixture(t)
	if _, e := f.c.Tick(context.Background(), f.now); e != nil {
		t.Fatal(e)
	}
	f.counts(t, 0, 0)
	if f.replay(t) != "" {
		t.Fatal("empty boot")
	}
	f.config.Limits.MaxDatabaseBytes = 1
	f.c.Config = f.config
	if _, e := f.c.Tick(context.Background(), f.now.Add(time.Minute)); e == nil {
		t.Fatal("database limit ignored")
	}
	f.c.Config.Limits.MaxDatabaseBytes = 1610612736
	f.r.fail = true
	f.c.dirty = true
	if f.c.Export(context.Background()) == nil {
		t.Fatal("renderer failure ignored")
	}
	if !f.c.Healthy(time.Now().Add(60 * time.Second)) {
		t.Fatal("backend retry caused liveness failure")
	}
}

// Ensure the schema driver and fixture remain independently usable for migration tests.
var _ *sql.DB
