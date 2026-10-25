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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
	"github.com/zekihan/accessrelay/internal/renderer"
	"github.com/zekihan/accessrelay/internal/source"
	"github.com/zekihan/accessrelay/internal/store"
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
	raw, _ := json.Marshal(map[string]any{"url": f.backend.URL, "allowInsecureHTTP": true})
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
	f.c.renderRetry = time.Time{}
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
	if err := f.c.checkStorage(); err == nil || err.Error() != "database_limit" {
		t.Fatal("database limit missing from direct storage check")
	}
	f.c.Config.Limits.MaxDatabaseBytes = 1610612736
	f.r.fail = true
	f.c.dirty = true
	if f.c.Export(context.Background()) == nil {
		t.Fatal("renderer failure ignored")
	}
	starts := f.r.starts
	if f.c.Export(context.Background()) == nil || f.r.starts != starts {
		t.Fatal("renderer retry was not throttled", f.r.starts, starts)
	}
	f.r.fail = false
	f.c.renderRetry = time.Time{}
	if err := f.c.Export(context.Background()); err != nil {
		t.Fatal("renderer did not recover after retry deadline:", err)
	}
	if !f.c.Healthy(time.Now().Add(60 * time.Second)) {
		t.Fatal("backend retry caused liveness failure")
	}
}

func TestDatabaseBudgetAllowsReusingPrunedPages(t *testing.T) {
	f := newFixture(t)
	var pageSize int64
	if err := f.c.Store.DB.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Store.DB.Exec(`PRAGMA max_page_count=80`); err != nil {
		t.Fatal(err)
	}
	message := strings.Repeat("x", 2000)
	for i := 0; i < 1000; i++ {
		digest := []byte{byte(i >> 8), byte(i)}
		if _, err := f.c.Store.DB.Exec(`INSERT INTO events(digest,time_ns,message,occurrences) VALUES(?,0,?,1)`, digest, message); err != nil {
			break
		}
	}
	var pageCount int64
	if err := f.c.Store.DB.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil || pageCount == 0 {
		t.Fatal(pageCount, err)
	}
	f.c.Config.Limits.MaxDatabaseBytes = pageCount * pageSize
	if _, err := f.c.Store.Prune(1); err != nil {
		t.Fatal(err)
	}
	var freelist int64
	if err := f.c.Store.DB.QueryRow(`PRAGMA freelist_count`).Scan(&freelist); err != nil || freelist == 0 {
		t.Fatal(freelist, err)
	}
	physical, err := f.c.Store.Size()
	if err != nil || physical < f.c.Config.Limits.MaxDatabaseBytes {
		t.Fatal(physical, err)
	}
	if err = f.c.checkStorage(); err != nil {
		t.Fatal("reusable pages blocked by physical database size:", err)
	}
	if _, err = f.c.Store.DB.Exec(`INSERT INTO events(digest,time_ns,message,occurrences) VALUES(?,2,?,1)`, []byte("reuse"), message); err != nil {
		t.Fatal("SQLite did not reuse freed pages:", err)
	}
}

func TestNewRejectsLowerDatabaseBudgetBelowPrunedHighWaterMark(t *testing.T) {
	f := newFixture(t)
	message := strings.Repeat("x", 2000)
	for i := 0; i < 1000; i++ {
		digest := []byte{byte(i >> 8), byte(i)}
		if _, err := f.c.Store.DB.Exec(`INSERT INTO events(digest,time_ns,message,occurrences) VALUES(?,0,?,1)`, digest, message); err != nil {
			t.Fatal("failed to build high-water database:", err)
		}
	}
	if _, err := f.c.Store.Prune(1); err != nil {
		t.Fatal("failed to prune high-water database:", err)
	}
	var pageSize, pageCount, freePages int64
	if err := f.c.Store.DB.QueryRow(`PRAGMA page_size`).Scan(&pageSize); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Store.DB.QueryRow(`PRAGMA page_count`).Scan(&pageCount); err != nil {
		t.Fatal(err)
	}
	if err := f.c.Store.DB.QueryRow(`PRAGMA freelist_count`).Scan(&freePages); err != nil {
		t.Fatal(err)
	}
	if freePages == 0 || pageCount*pageSize <= 1<<20 {
		t.Fatalf("fixture did not exceed lowered budget with reusable pages: pages=%d free=%d pageSize=%d", pageCount, freePages, pageSize)
	}
	if err := f.c.Store.Close(); err != nil {
		t.Fatal(err)
	}

	lowered := f.config
	lowered.Limits.MaxDatabaseBytes = 1 << 20
	if _, err := New(lowered, f.client, f.r); err == nil || err.Error() != "database_limit" {
		t.Fatalf("expected opaque database_limit for lowered cap, got %v", err)
	}
}

func TestReclaimsOrphanRuntimeGenerationBeforeLimitCheck(t *testing.T) {
	f := newFixture(t)
	f.c.Config.Limits.MaxRuntimeBytes = 1 << 20
	name := "0123456789abcdef0123456789abcdef"
	dir := filepath.Join(f.config.RuntimeDir, "generations", name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "access.log"), make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.c.checkStorage(); err != nil {
		t.Fatal("unowned generation caused permanent runtime pressure:", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("orphan generation remains", err)
	}
}

func TestTickKeepsCollectingWhenRendererCannotStart(t *testing.T) {
	f := newFixture(t)
	f.rows = []map[string]string{f.row("2026-10-01T00:01:00Z", "a")}
	f.r.fail = true
	if _, err := f.c.Tick(context.Background(), f.now); err != nil {
		t.Fatal("render failure interrupted collection:", err)
	}
	f.counts(t, 1, 1)
	var cursor int64
	if err := f.c.Store.Get("cursor", &cursor); err != nil || cursor == 0 {
		t.Fatal(cursor, err)
	}
	if err := f.c.Refresh(f.now); err != nil {
		t.Fatal(err)
	}
	status := f.c.Snapshot()
	if status.ExportFailures == 0 || status.Error == nil || *status.Error != "renderer_start" {
		t.Fatal(status.ExportFailures, status.Error)
	}
}

type recordingBackend struct {
	Backend
	calls [][2]int64
}

func (b *recordingBackend) Fetch(ctx context.Context, start, end int64) (source.Result, error) {
	b.calls = append(b.calls, [2]int64{start, end})
	return b.Backend.Fetch(ctx, start, end)
}

func TestReconciliationGetsTurnsDuringForwardBacklog(t *testing.T) {
	f := newFixture(t)
	f.config.Collection.PollInterval = "1s"
	f.config.Collection.ReconciliationInterval = "1s"
	f.config.Collection.BackfillWindow = "1m"
	f.config.Collection.IngestionDelay = "30s"
	f.c.Config = f.config
	backend := &recordingBackend{Backend: f.client}
	f.c.Backend = backend
	start := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	for i := range 4 {
		if _, err := f.c.Tick(context.Background(), start.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if len(backend.calls) < 3 {
		t.Fatal("too few query windows", len(backend.calls))
	}
	if backend.calls[1][0] >= backend.calls[0][1] {
		t.Fatal("reconciliation did not revisit committed history", backend.calls)
	}
	if backend.calls[2][0] <= backend.calls[1][0] {
		t.Fatal("forward polling did not resume after a reconciliation turn", backend.calls)
	}
}

type minimumSplitter struct{ Backend }

func (s minimumSplitter) Fetch(ctx context.Context, start, end int64) (source.Result, error) {
	if end-start > int64(time.Second) {
		return source.Result{}, &source.QueryError{Reason: "timeout", Split: true}
	}
	return s.Backend.Fetch(ctx, start, end)
}

func TestTimeoutSplitReachesConfiguredMinimumWindow(t *testing.T) {
	f := newFixture(t)
	f.c.Config.Collection.MaxWindowQueries = 1024
	f.c.Backend = minimumSplitter{Backend: f.client}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	end := start + int64(300*time.Second)
	if err := f.c.Window(context.Background(), start, end, true); err != nil {
		t.Fatal(err)
	}
	var cursor int64
	if err := f.c.Store.Get("cursor", &cursor); err != nil || cursor != end {
		t.Fatal(cursor, err)
	}
}

type splitThenFailBackend struct {
	Backend
	failStart int64
}

func (b splitThenFailBackend) Fetch(ctx context.Context, start, end int64) (source.Result, error) {
	if end-start > 2*int64(time.Second) {
		return source.Result{}, &source.QueryError{Reason: "timeout", Split: true}
	}
	if start >= b.failStart {
		return source.Result{}, &source.QueryError{Reason: "backend_request"}
	}
	return b.Backend.Fetch(ctx, start, end)
}

func TestSplitContinuesAfterRenderFailureWithoutMovingFailedReconcilePointer(t *testing.T) {
	f := newFixture(t)
	if err := f.c.Export(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.config.RuntimeDir, "generations", f.c.Generation, "access.log")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	middle, end := start+2*int64(time.Second), start+4*int64(time.Second)
	f.rows = []map[string]string{f.row("2026-10-01T00:00:01Z", "a")}
	f.c.Backend = splitThenFailBackend{Backend: f.client, failStart: middle}
	f.c.reconcileStart, f.c.reconcileEnd, f.c.reconcileTurn = start, end, true
	if err := f.c.runReconcileWindow(context.Background(), f.now, int64(4*time.Second)); err == nil {
		t.Fatal("second half backend failure was hidden")
	}
	if f.c.reconcileStart != start {
		t.Fatal("failed reconciliation advanced", f.c.reconcileStart)
	}
	var cursor int64
	if err := f.c.Store.Get("cursor", &cursor); err != nil || cursor != 0 {
		t.Fatal("reconciliation changed the forward cursor", cursor, err)
	}
	f.counts(t, 1, 1)
}

type failFirstBackend struct {
	Backend
	calls [][2]int64
}

func (b *failFirstBackend) Fetch(ctx context.Context, start, end int64) (source.Result, error) {
	b.calls = append(b.calls, [2]int64{start, end})
	if len(b.calls) == 1 {
		return source.Result{}, &source.QueryError{Reason: "backend_request"}
	}
	return b.Backend.Fetch(ctx, start, end)
}

func TestFailedReconciliationYieldsToForwardPolling(t *testing.T) {
	f := newFixture(t)
	start := HistoryStart(f.now, 1)
	if err := f.c.Window(context.Background(), start, start+int64(time.Minute), true); err != nil {
		t.Fatal(err)
	}
	var cursor int64
	if err := f.c.Store.Get("cursor", &cursor); err != nil {
		t.Fatal(err)
	}
	backend := &failFirstBackend{Backend: f.client}
	f.c.Backend = backend
	f.c.timeReconcile = f.now
	if _, err := f.c.Tick(context.Background(), f.now); err == nil {
		t.Fatal("reconciliation backend failure was hidden")
	}
	if _, err := f.c.Tick(context.Background(), f.now.Add(time.Second)); err != nil {
		t.Fatal("forward collection did not resume:", err)
	}
	if len(backend.calls) != 2 || backend.calls[1][0] != cursor {
		t.Fatal("forward window did not get the next turn", backend.calls, cursor)
	}
}

// Ensure the schema driver and fixture remain independently usable for migration tests.
var _ *sql.DB

type failedShutdownConsumer struct{ consumer }

func (r *failedShutdownConsumer) Stop(context.Context) error {
	return errors.New("renderer_diagnostic_storage")
}

func TestShutdownReportsRendererFailureAndReleasesStore(t *testing.T) {
	f := newFixture(t)
	f.c.Consumer = &failedShutdownConsumer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.c.Run(ctx); err == nil || err.Error() != "renderer_shutdown" {
		t.Fatalf("shutdown error = %v", err)
	}
	if err := f.c.Store.DB.Ping(); err == nil {
		t.Fatal("database remained open after renderer shutdown failure")
	}
	replacement, err := New(f.config, f.client, &consumer{})
	if err != nil {
		t.Fatal("writer lock was not released:", err)
	}
	if err := replacement.Store.Close(); err != nil {
		t.Fatal(err)
	}
}

func identityConfig(root string) config.Config {
	c := config.Default()
	c.StateDir = filepath.Join(root, "state")
	c.RuntimeDir = filepath.Join(root, "runtime")
	c.ConnectionFile = filepath.Join(root, "connection.json")
	c.Collection.Source = config.Source{Cluster: "cluster-a", Namespace: "namespace-a", Container: "container-a"}
	return c
}

func TestSourceIdentityMismatchPreservesDatabaseAndReleasesLock(t *testing.T) {
	for _, field := range []string{"cluster", "namespace", "container"} {
		t.Run(field, func(t *testing.T) {
			c := identityConfig(t.TempDir())
			first, err := New(c, nil, &consumer{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = first.Store.DB.Exec(`INSERT INTO sources(id,identity) VALUES(1,'{}');
				INSERT INTO events(id,digest,time_ns,source_id,message,occurrences) VALUES(1,zeroblob(32),123,1,'fixture',1);
				INSERT INTO changes(seq,event_id,copies) VALUES(1,1,1);
				INSERT INTO metadata(key,value) VALUES('cursor','123')`)
			if err != nil {
				t.Fatal(err)
			}
			if err = first.Store.Close(); err != nil {
				t.Fatal(err)
			}
			changed := c
			switch field {
			case "cluster":
				changed.Collection.Source.Cluster = "cluster-b"
			case "namespace":
				changed.Collection.Source.Namespace = "namespace-b"
			case "container":
				changed.Collection.Source.Container = "container-b"
			}
			if _, err = New(changed, nil, &consumer{}); err == nil || err.Error() != "source_mismatch" {
				t.Fatalf("New error = %v, want source_mismatch", err)
			}
			// Failed startup must release the lock and leave data/cursor untouched.
			reopened, err := New(c, nil, &consumer{})
			if err != nil {
				t.Fatal("writer lock was not released:", err)
			}
			defer reopened.Store.Close()
			var cursor int64
			if err = reopened.Store.Get("cursor", &cursor); err != nil || cursor != 123 {
				t.Fatalf("cursor = %d, err = %v", cursor, err)
			}
			events, copies, err := reopened.Store.Counts()
			if err != nil || events != 1 || copies != 1 {
				t.Fatalf("events/copies = %d/%d, err = %v", events, copies, err)
			}
		})
	}
}

func TestLegacyNonemptyDatabaseAdoptsConfiguredSource(t *testing.T) {
	c := identityConfig(t.TempDir())
	legacy, err := store.Open(c.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.DB.Exec(`INSERT INTO sources(id,identity) VALUES(1,'{}');
		INSERT INTO events(id,digest,time_ns,source_id,message,occurrences) VALUES(1,zeroblob(32),123,1,'legacy',2);
		INSERT INTO changes(seq,event_id,copies) VALUES(1,1,2);
		INSERT INTO metadata(key,value) VALUES('cursor','123')`)
	if err != nil {
		t.Fatal(err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := New(c, nil, &consumer{})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Store.Close()
	events, copies, err := opened.Store.Counts()
	if err != nil || events != 1 || copies != 2 {
		t.Fatalf("legacy events/copies = %d/%d, err = %v", events, copies, err)
	}
	var bound sourceIdentity
	if err = opened.Store.Get("source_identity", &bound); err != nil || bound != (sourceIdentity{"cluster-a", "namespace-a", "container-a"}) {
		t.Fatalf("bound identity = %+v, err = %v", bound, err)
	}
}

func TestMalformedSourceIdentityRejectedAndLockReleased(t *testing.T) {
	c := identityConfig(t.TempDir())
	opened, err := New(c, nil, &consumer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = opened.Store.DB.Exec(`UPDATE metadata SET value='{"cluster":"cluster-a","namespace":null,"container":"container-a"}' WHERE key='source_identity'`); err != nil {
		t.Fatal(err)
	}
	if err = opened.Store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = New(c, nil, &consumer{}); err == nil || err.Error() != "database_metadata" {
		t.Fatalf("New error = %v, want database_metadata", err)
	}
	reopened, err := store.Open(c.StateDir)
	if err != nil {
		t.Fatal("writer lock was not released:", err)
	}
	_ = reopened.Close()
}

type backendFunc func(context.Context, int64, int64) (source.Result, error)

func (f backendFunc) Fetch(ctx context.Context, start, end int64) (source.Result, error) {
	return f(ctx, start, end)
}

func TestRootWindowQueryBudgetStopsMaliciousSplitTree(t *testing.T) {
	f := newFixture(t)
	f.c.Config.Collection.MaxWindowQueries = 7
	calls := 0
	f.c.Backend = backendFunc(func(context.Context, int64, int64) (source.Result, error) {
		calls++
		return source.Result{}, &source.QueryError{Reason: "window_limit", Split: true}
	})
	start := f.now.UnixNano()
	err := f.c.Window(context.Background(), start, start+int64(time.Hour), true)
	if err == nil || err.Error() != "window_budget" || calls != 7 || f.c.Snapshot().Queries != 7 {
		t.Fatalf("split tree escaped shared query budget: calls=%d err=%v", calls, err)
	}
	var cursor int64
	if err := f.c.Store.Get("cursor", &cursor); err != nil || cursor != 0 {
		t.Fatalf("failed tree advanced cursor: %d %v", cursor, err)
	}
}

func TestRootWindowBudgetsIncludeDiscardedStagesAndBothChildren(t *testing.T) {
	for _, limit := range []string{"bytes", "records", "ordinary"} {
		t.Run(limit, func(t *testing.T) {
			f := newFixture(t)
			start := f.now.UnixNano()
			row := f.row(source.Timestamp(start), "fixture")
			raw, _ := json.Marshal(row)
			lineBytes := len(raw) + 1
			parentRecords := 1024/lineBytes + 1
			parentBytes := int64(parentRecords * lineBytes)
			var calls int
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				_ = r.ParseForm()
				a, _ := source.ParseTimestamp(r.Form.Get("start"))
				b, _ := source.ParseTimestamp(r.Form.Get("end"))
				n := 1
				if b-a > int64(2*time.Second) {
					n = parentRecords // oversize root is discarded before splitting
				}
				for range n {
					x := f.row(source.Timestamp(a), "fixture")
					_ = json.NewEncoder(w).Encode(x)
				}
			}))
			defer backend.Close()
			raw, _ = json.Marshal(map[string]any{"url": backend.URL, "allowInsecureHTTP": true})
			if err := os.WriteFile(f.config.ConnectionFile, raw, 0600); err != nil {
				t.Fatal(err)
			}
			f.client.Config.Collection.MaxWindowBytes = 1024
			f.client.Config.Collection.MaxLineBytes = 1024
			switch limit {
			case "bytes":
				f.c.Config.Collection.MaxWindowTotalBytes = parentBytes + int64(lineBytes)
			case "records":
				f.c.Config.Collection.MaxWindowRecords = int64(parentRecords) // discarded parent + left
			}
			end := start + int64(4*time.Second)
			err := f.c.Window(context.Background(), start, end, true)
			var cursor int64
			_ = f.c.Store.Get("cursor", &cursor)
			if limit == "ordinary" {
				if err != nil || calls != 3 || cursor != end {
					t.Fatalf("ordinary adaptive split failed: %v calls=%d cursor=%d", err, calls, cursor)
				}
				f.counts(t, 2, 2)
			} else {
				if err == nil || err.Error() != "window_budget" || calls != 3 || cursor != start+int64(2*time.Second) {
					t.Fatalf("%s budget reset across children: %v calls=%d cursor=%d", limit, err, calls, cursor)
				}
				f.counts(t, 1, 1)
			}
			stages, _ := filepath.Glob(filepath.Join(f.config.RuntimeDir, "window-*"))
			if len(stages) != 0 {
				t.Fatal("stages leaked", stages)
			}
		})
	}
}

func TestRootWindowDeadlineIsSharedAcrossChildren(t *testing.T) {
	f := newFixture(t)
	f.c.Config.Collection.WindowTimeout = "1s"
	calls := 0
	var rootDeadline time.Time
	f.c.Backend = backendFunc(func(ctx context.Context, start, end int64) (source.Result, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("missing root deadline")
		}
		if calls == 1 {
			rootDeadline = deadline
			timer := time.NewTimer(100 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
			return source.Result{}, &source.QueryError{Reason: "timeout", Split: true}
		}
		if deadline != rootDeadline {
			t.Fatal("child reset root deadline")
		}
		if calls == 2 {
			return f.client.Fetch(ctx, start, end)
		}
		<-ctx.Done()
		return source.Result{}, &source.QueryError{Reason: "timeout", Split: true}
	})
	start := f.now.UnixNano()
	began := time.Now()
	err := f.c.Window(context.Background(), start, start+int64(4*time.Second), true)
	if err == nil || err.Error() != "window_budget" || calls != 3 || time.Since(began) > 3*time.Second {
		t.Fatalf("deadline escaped: %v calls=%d elapsed=%v", err, calls, time.Since(began))
	}
	var cursor int64
	_ = f.c.Store.Get("cursor", &cursor)
	if cursor != start+int64(2*time.Second) {
		t.Fatalf("deadline advanced over failed child: %d", cursor)
	}
}
