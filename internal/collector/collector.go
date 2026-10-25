// Package collector coordinates durable windows, replay and UTC retention.
package collector

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zekihan/accessrelay/internal/config"
	"github.com/zekihan/accessrelay/internal/renderer"
	"github.com/zekihan/accessrelay/internal/source"
	"github.com/zekihan/accessrelay/internal/store"
	"golang.org/x/sys/unix"
)

type Backend interface {
	Fetch(context.Context, int64, int64) (source.Result, error)
}
type Consumer interface {
	Start(context.Context, string) error
	Stop(context.Context) error
	NeedsReplay() bool
	Generation() string
	Snapshot() renderer.Snapshot
}
type Status struct {
	Heartbeat                    float64     `json:"heartbeat"`
	Initialized                  bool        `json:"initialized"`
	LastSuccessfulQuery          *float64    `json:"lastSuccessfulQuery"`
	Cursor                       *string     `json:"cursor"`
	LagSeconds                   *float64    `json:"lagSeconds"`
	Stale                        bool        `json:"stale"`
	Error                        *string     `json:"error"`
	SkippedOperationalLastWindow int64       `json:"skippedOperationalLastWindow"`
	StorageUsedPercent           float64     `json:"storageUsedPercent"`
	StoragePressure              bool        `json:"storagePressure"`
	DatabaseBytes                int64       `json:"databaseBytes"`
	UnrecoverableIntervals       []store.Gap `json:"unrecoverableIntervals"`
	Queries                      uint64      `json:"queries"`
	Retries                      uint64      `json:"retries"`
	CommittedWindows             uint64      `json:"committedWindows"`
	CommittedBytes               uint64      `json:"committedBytes"`
	ExportFailures               uint64      `json:"exportFailures"`
}
type Collector struct {
	Config                       config.Config
	Store                        *store.Store
	Backend                      Backend
	Consumer                     Consumer
	Generation                   string
	exported                     int64
	dirty                        bool
	cutoff                       int64
	nextPoll, timeReconcile      time.Time
	reconcileStart, reconcileEnd int64
	failure                      string
	renderFailure                string
	renderRetry                  time.Time
	renderBackoff                time.Duration
	reconcileTurn                bool
	forwardTurn                  bool
	lastLoop                     time.Time
	mu                           sync.RWMutex
	status                       Status
}

func New(c config.Config, backend Backend, consumer Consumer) (*Collector, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if os.MkdirAll(filepath.Join(c.RuntimeDir, "generations"), 0700) != nil {
		return nil, errors.New("runtime_storage")
	}
	s, err := store.Open(c.StateDir)
	if err != nil {
		return nil, err
	}
	var pageSize int64
	if s.DB.QueryRow(`PRAGMA page_size`).Scan(&pageSize) != nil || pageSize <= 0 {
		_ = s.Close()
		return nil, errors.New("database_format")
	}
	requestedPageCount := c.Limits.MaxDatabaseBytes / pageSize
	var effectivePageCount int64
	if err = s.DB.QueryRow(fmt.Sprintf("PRAGMA max_page_count=%d", requestedPageCount)).Scan(&effectivePageCount); err != nil || effectivePageCount != requestedPageCount {
		_ = s.Close()
		return nil, errors.New("database_limit")
	}
	if err := bindSourceIdentity(s.DB, c.Collection.Source); err != nil {
		_ = s.Close()
		return nil, err
	}
	collector := &Collector{Config: c, Store: s, Backend: backend, Consumer: consumer, dirty: true, lastLoop: time.Now()}
	collector.status.Stale = true
	// Incomplete response stages and unpublished atomic writes are disposable.
	entries, _ := os.ReadDir(c.RuntimeDir)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "window-") || strings.HasPrefix(entry.Name(), ".publish-") {
			_ = os.Remove(filepath.Join(c.RuntimeDir, entry.Name()))
		}
	}
	return collector, nil
}

type sourceIdentity struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Container string `json:"container"`
}

// bindSourceIdentity pins an existing database to its configured log source.
// An unbound inherited database is adopted on first startup; its original
// source cannot be recovered from the inherited schema.
func bindSourceIdentity(db *sql.DB, configured config.Source) error {
	want := sourceIdentity{
		Cluster: configured.Cluster, Namespace: configured.Namespace, Container: configured.Container,
	}
	tx, err := db.Begin()
	if err != nil {
		return errors.New("database_metadata")
	}
	defer func() { _ = tx.Rollback() }()
	var raw string
	err = tx.QueryRow(`SELECT value FROM metadata WHERE key='source_identity'`).Scan(&raw)
	if err == sql.ErrNoRows {
		encoded, marshalErr := json.Marshal(want)
		if marshalErr != nil {
			return errors.New("database_metadata")
		}
		if _, err = tx.Exec(`INSERT INTO metadata(key,value) VALUES('source_identity',?)`, string(encoded)); err != nil {
			return errors.New("database_metadata")
		}
	} else if err != nil {
		return errors.New("database_metadata")
	} else {
		var got sourceIdentity
		if json.Unmarshal([]byte(raw), &got) != nil || got.Cluster == "" || got.Namespace == "" || got.Container == "" {
			return errors.New("database_metadata")
		}
		if got != want {
			return errors.New("source_mismatch")
		}
	}
	if tx.Commit() != nil {
		return errors.New("database_metadata")
	}
	return nil
}

func HistoryStart(now time.Time, days int) int64 {
	u := now.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -days+1).UnixNano()
}
func (c *Collector) Snapshot() Status { c.mu.RLock(); defer c.mu.RUnlock(); return c.status }
func (c *Collector) Healthy(now time.Time) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return now.Sub(c.lastLoop) < config.Duration(c.Config.Collection.WindowTimeout)+90*time.Second
}
func (c *Collector) Refresh(now time.Time) error {
	var last *float64
	var cursor int64
	var gaps []store.Gap
	if err := c.Store.Get("last_success", &last); err != nil {
		return err
	}
	if err := c.Store.Get("cursor", &cursor); err != nil {
		return err
	}
	if err := c.Store.Get("gaps", &gaps); err != nil {
		return err
	}
	size, err := c.Store.Size()
	if err != nil {
		return errors.New("storage_read")
	}
	var pageSize, pageCount, freePages int64
	if c.Store.DB.QueryRow(`PRAGMA page_size`).Scan(&pageSize) != nil ||
		c.Store.DB.QueryRow(`PRAGMA page_count`).Scan(&pageCount) != nil ||
		c.Store.DB.QueryRow(`PRAGMA freelist_count`).Scan(&freePages) != nil || pageSize <= 0 || pageCount < freePages {
		return errors.New("storage_read")
	}
	disk := unix.Statfs_t{}
	if unix.Statfs(c.Config.StateDir, &disk) != nil {
		return errors.New("storage_read")
	}
	free := uint64(disk.Bavail) * uint64(disk.Bsize)
	total := uint64(disk.Blocks) * uint64(disk.Bsize)
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &c.status
	s.Heartbeat = float64(now.UnixNano()) / 1e9
	s.Initialized = c.Generation != ""
	s.LastSuccessfulQuery = last
	s.Stale = last == nil || s.Heartbeat-*last > config.Duration(c.Config.Collection.StaleAfter).Seconds()
	s.Cursor = nil
	s.LagSeconds = nil
	if cursor != 0 {
		stamp := source.Timestamp(cursor)
		lag := max(0, now.Sub(time.Unix(0, cursor)).Seconds())
		s.Cursor = &stamp
		s.LagSeconds = &lag
	}
	s.Error = nil
	if c.failure != "" {
		e := c.failure
		s.Error = &e
	} else if c.renderFailure != "" {
		e := c.renderFailure
		s.Error = &e
	}
	s.SkippedOperationalLastWindow = c.status.SkippedOperationalLastWindow
	s.DatabaseBytes = size
	s.StoragePressure = (pageCount-freePages)*pageSize >= c.Config.Limits.MaxDatabaseBytes || free < c.Config.Limits.ReserveBytes
	if total > 0 {
		s.StorageUsedPercent = 100 * (1 - float64(free)/float64(total))
		s.StoragePressure = s.StoragePressure || float64(free)/float64(total) < 0.3
	}
	s.UnrecoverableIntervals = gaps
	if gaps == nil {
		s.UnrecoverableIntervals = []store.Gap{}
	}
	return nil
}
func (c *Collector) checkStorage() error {
	if err := c.reclaimUnownedGenerations(); err != nil {
		return err
	}
	sizeRuntime, err := TreeSize(c.Config.RuntimeDir)
	if err != nil {
		return errors.New("runtime_storage")
	}
	if sizeRuntime >= c.Config.Limits.MaxRuntimeBytes {
		return errors.New("runtime_limit")
	}

	var pageSize, pageCount, freePages int64
	if c.Store.DB.QueryRow(`PRAGMA page_size`).Scan(&pageSize) != nil ||
		c.Store.DB.QueryRow(`PRAGMA page_count`).Scan(&pageCount) != nil ||
		c.Store.DB.QueryRow(`PRAGMA freelist_count`).Scan(&freePages) != nil || pageSize <= 0 || pageCount < freePages {
		return errors.New("storage_read")
	}
	if pageCount-freePages >= c.Config.Limits.MaxDatabaseBytes/pageSize {
		return errors.New("database_limit")
	}
	for _, dir := range []string{c.Config.StateDir, c.Config.RuntimeDir} {
		var st unix.Statfs_t
		if unix.Statfs(dir, &st) != nil {
			return errors.New("storage_read")
		}
		if uint64(st.Bavail)*uint64(st.Bsize) < c.Config.Limits.ReserveBytes {
			return errors.New("storage_reserve")
		}
	}
	return nil
}

// reclaimUnownedGenerations removes directories only after the consumer has
// released them. Renderer.Generation remains set while its child owns a replay
// and is cleared after that child exits; its parent-death signal covers a
// collector crash.
func (c *Collector) reclaimUnownedGenerations() error {
	dir := filepath.Join(c.Config.RuntimeDir, "generations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.New("runtime_storage")
	}
	owned := c.Consumer.Generation()
	for _, entry := range entries {
		name := entry.Name()
		if name == owned || !ownedGenerationName(name) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
			return errors.New("cleanup_storage")
		}
		if name == c.Generation {
			c.Generation = ""
			c.exported = 0
			c.dirty = true
		}
	}
	return nil
}

func ownedGenerationName(name string) bool {
	if len(name) != 32 {
		return false
	}
	for _, r := range name {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
func (c *Collector) rotate(ctx context.Context) error {
	// Stop before snapshotting: the old consumer must never see a partially rebuilt replay.
	if err := c.Consumer.Stop(ctx); err != nil {
		return err
	}
	if err := c.reclaimUnownedGenerations(); err != nil {
		return err
	}
	runtimeSize, err := TreeSize(c.Config.RuntimeDir)
	if err != nil || runtimeSize+c.Config.Limits.MaxReplayBytes > c.Config.Limits.MaxRuntimeBytes {
		return errors.New("runtime_limit")
	}

	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return errors.New("generation_identifier")
	}
	generation := hex.EncodeToString(token[:])
	dir := filepath.Join(c.Config.RuntimeDir, "generations", generation)
	if os.Mkdir(dir, 0700) != nil {
		return errors.New("export_storage")
	}
	completed := false
	defer func() {
		if !completed {
			_ = os.RemoveAll(dir)
		}
	}()
	f, err := os.OpenFile(filepath.Join(dir, "access.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("export_storage")
	}
	seq, err := c.Store.Export(f, 0, true, c.Config.Limits.MaxReplayBytes)
	if err != nil {
		_ = f.Close()
		return err
	}
	if f.Sync() != nil {
		_ = f.Close()
		return errors.New("export_sync")
	}
	if f.Close() != nil {
		return errors.New("export_close")
	}
	if err = c.Consumer.Start(ctx, generation); err != nil {
		return err
	}
	c.Generation = generation
	c.exported = seq
	c.dirty = false
	completed = true
	return nil
}
func (c *Collector) Cleanup() error {
	if c.Consumer.Generation() != c.Generation || !c.Consumer.Snapshot().Ready {
		return nil
	}
	dir := filepath.Join(c.Config.RuntimeDir, "generations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.New("cleanup_storage")
	}
	for _, entry := range entries {
		if entry.Name() != c.Generation {
			if err = os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
				return errors.New("cleanup_storage")
			}
		}
	}
	return nil
}
func (c *Collector) Export(ctx context.Context) error {
	if c.renderFailure != "" && time.Now().Before(c.renderRetry) {
		return errors.New(c.renderFailure)
	}
	needsReplay := c.Consumer.NeedsReplay()
	if needsReplay {
		c.dirty = true
	}
	if c.dirty {
		if !needsReplay && c.Consumer.Generation() == "" {
			return nil
		}
		err := c.rotate(ctx)
		if err != nil {
			c.recordRenderFailure(err)
			return err
		}
		c.clearRenderFailure()
		return nil
	}
	if c.Consumer.Generation() == "" {
		return nil
	}
	path := filepath.Join(c.Config.RuntimeDir, "generations", c.Generation, "access.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err == nil {
		info, e := f.Stat()
		if e != nil {
			err = errors.New("export_storage")
		} else {
			var seq int64
			seq, err = c.Store.Export(f, c.exported, false, c.Config.Limits.MaxReplayBytes-info.Size())
			if err == nil {
				err = f.Sync()
			}
			if err == nil {
				c.exported = seq
			}
		}
		if e := f.Close(); err == nil {
			err = e
		}
	}
	if err != nil {
		c.dirty = true
		c.recordRenderFailure(errors.New("export_failed"))
		_ = c.Consumer.Stop(ctx)
		return errors.New("export_failed")
	}
	c.clearRenderFailure()
	return nil
}

func (c *Collector) recordRenderFailure(err error) {
	c.renderFailure = err.Error()
	if c.renderBackoff == 0 {
		c.renderBackoff = time.Second
	} else {
		c.renderBackoff = min(time.Minute, c.renderBackoff*2)
	}
	c.renderRetry = time.Now().Add(c.renderBackoff)
	c.mu.Lock()
	c.status.ExportFailures++
	c.mu.Unlock()
}

func (c *Collector) clearRenderFailure() {
	c.renderFailure = ""
	c.renderRetry = time.Time{}
	c.renderBackoff = 0
}

type postCommitRenderError struct{ err error }

func (e postCommitRenderError) Error() string { return e.err.Error() }
func (e postCommitRenderError) Unwrap() error { return e.err }

func (c *Collector) Window(ctx context.Context, start, end int64, advance bool) error {
	ctx, cancel := source.WithWindowBudget(ctx, c.Config.Collection)
	defer cancel()
	err := c.window(ctx, start, end, advance)
	if budgetErr := source.WindowBudgetFrom(ctx).Check(ctx); budgetErr != nil {
		return budgetErr
	}
	return err
}

func (c *Collector) window(ctx context.Context, start, end int64, advance bool) error {
	if err := source.WindowBudgetFrom(ctx).BeginQuery(ctx); err != nil {
		return err
	}
	if err := c.checkStorage(); err != nil {
		return err
	}
	c.mu.Lock()
	c.status.Queries++
	c.lastLoop = time.Now()
	c.mu.Unlock()
	result, err := c.Backend.Fetch(ctx, start, end)
	if budgetErr := source.WindowBudgetFrom(ctx).Check(ctx); budgetErr != nil {
		if err == nil {
			_ = os.Remove(result.Path)
		}
		return budgetErr
	}
	if err != nil {
		var q *source.QueryError
		minimum := int64(config.Duration(c.Config.Collection.MinimumWindow))
		if errors.As(err, &q) && q.Split && minimum > 0 && end-start > minimum {
			duration := end - start
			left := (duration / 2 / minimum) * minimum
			if left < minimum {
				left = minimum
			}
			if duration >= 2*minimum && duration-left < minimum {
				left = duration - minimum
			}
			middle := start + left
			firstErr := c.window(ctx, start, middle, advance)
			var firstRenderErr postCommitRenderError
			if firstErr != nil && !errors.As(firstErr, &firstRenderErr) {
				return firstErr
			}
			secondErr := c.window(ctx, middle, end, advance)
			if secondErr != nil {
				return secondErr
			}
			return firstErr
		}
		return err
	}
	defer func() { _ = os.Remove(result.Path) }()
	if err = c.Store.CommitWindow(ctx, result.Path, end, advance, float64(time.Now().UnixNano())/1e9); err != nil {
		return err
	}
	c.mu.Lock()
	c.status.SkippedOperationalLastWindow = result.Operational
	c.status.CommittedWindows++
	c.status.CommittedBytes += uint64(result.Bytes)
	c.mu.Unlock()
	if err = c.Export(ctx); err != nil {
		return postCommitRenderError{err: err}
	}
	return c.Cleanup()
}
func (c *Collector) Tick(ctx context.Context, now time.Time) (bool, error) {
	cutoff := HistoryStart(now, c.Config.Report.HistoryDays)
	target := now.Add(-config.Duration(c.Config.Collection.IngestionDelay)).UnixNano()
	if c.cutoff != cutoff {
		if _, err := c.Store.Prune(cutoff); err != nil {
			return false, err
		}
		c.cutoff = cutoff
		c.dirty = true
		c.reconcileEnd = 0
		c.timeReconcile = time.Time{}
		c.forwardTurn = false
	}
	cursor := cutoff
	if err := c.Store.Get("cursor", &cursor); err != nil {
		return false, err
	}
	if cursor < cutoff {
		if err := c.Store.Gap(source.Timestamp(cursor), source.Timestamp(cutoff), cutoff); err != nil {
			return false, err
		}
		cursor = cutoff
	}
	_ = c.Export(ctx)
	if err := c.Cleanup(); err != nil {
		return false, err
	}
	size := int64(config.Duration(c.Config.Collection.BackfillWindow))
	forwardDue := cursor < target && (target-cursor > size || !now.Before(c.nextPoll))
	if c.reconcileEnd == 0 && !now.Before(c.timeReconcile) && (!c.forwardTurn || !forwardDue) {
		c.reconcileStart = cutoff
		c.reconcileEnd = min(cursor, target)
		c.reconcileTurn = c.reconcileEnd > c.reconcileStart
		if !c.reconcileTurn {
			c.reconcileEnd = 0
			c.timeReconcile = now.Add(config.Duration(c.Config.Collection.ReconciliationInterval))
		}
	}
	if c.reconcileTurn && c.reconcileEnd != 0 {
		if err := c.runReconcileWindow(ctx, now, size); err != nil {
			return false, err
		}
		return true, nil
	}
	if cursor < target && (target-cursor > size || !now.Before(c.nextPoll)) {
		end := min(cursor+size, target)
		start := cursor
		if target-cursor <= size {
			start = max(cutoff, cursor-int64(config.Duration(c.Config.Collection.Overlap)))
		}
		if err := c.Window(ctx, start, end, true); err != nil {
			var renderErr postCommitRenderError
			if !errors.As(err, &renderErr) {
				c.reconcileTurn = c.reconcileEnd != 0
				c.forwardTurn = false
				return false, err
			}
		}
		c.nextPoll = now.Add(config.Duration(c.Config.Collection.PollInterval))
		c.failure = ""
		c.reconcileTurn = c.reconcileEnd != 0
		c.forwardTurn = false
		return target-end > size, nil
	}
	if c.reconcileEnd != 0 {
		if err := c.runReconcileWindow(ctx, now, size); err != nil {
			return false, err
		}
		return c.reconcileEnd != 0, nil
	}
	return false, nil
}

func (c *Collector) runReconcileWindow(ctx context.Context, now time.Time, size int64) error {
	end := min(c.reconcileStart+size, c.reconcileEnd)
	if c.reconcileStart < end {
		if err := c.Window(ctx, c.reconcileStart, end, false); err != nil {
			var renderErr postCommitRenderError
			if !errors.As(err, &renderErr) {
				c.reconcileTurn = false
				c.forwardTurn = false
				return err
			}
		}
		c.reconcileStart = end
		c.reconcileTurn = false
		c.failure = ""
	}
	if c.reconcileStart >= c.reconcileEnd {
		c.reconcileEnd = 0
		c.reconcileTurn = false
		c.forwardTurn = true
		c.timeReconcile = now.Add(config.Duration(c.Config.Collection.ReconciliationInterval))
	}
	return nil
}
func (c *Collector) Run(ctx context.Context) error {
	backoff := time.Second
	for ctx.Err() == nil {
		c.mu.Lock()
		c.lastLoop = time.Now()
		c.mu.Unlock()
		immediate, err := c.Tick(ctx, time.Now())
		wait := time.Second
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			c.failure = err.Error()
			c.mu.Lock()
			c.status.Retries++
			c.mu.Unlock()
			wait = backoff
			backoff = min(60*time.Second, backoff*2)
		} else {
			backoff = time.Second
			if immediate {
				wait = 0
			}
		}
		if err = c.Refresh(time.Now()); err != nil {
			c.failure = "storage_read"
		}
		// Backoff is cancellable, and reports remain available during backend unavailability.
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
			}
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stopErr := c.Consumer.Stop(shutdown)
	closeErr := c.Store.Close()
	if stopErr != nil {
		return errors.New("renderer_shutdown")
	}
	return closeErr
}

func TreeSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}
