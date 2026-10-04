// Package collector coordinates durable windows, replay and UTC retention.
package collector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	if _, err = s.DB.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", c.Limits.MaxDatabaseBytes/pageSize)); err != nil {
		_ = s.Close()
		return nil, errors.New("database_limit")
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
func HistoryStart(now time.Time, days int) int64 {
	u := now.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -days+1).UnixNano()
}
func (c *Collector) Snapshot() Status { c.mu.RLock(); defer c.mu.RUnlock(); return c.status }
func (c *Collector) Healthy(now time.Time) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return now.Sub(c.lastLoop) < config.Duration(c.Config.Collection.QueryTimeout)+90*time.Second
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
	}
	s.SkippedOperationalLastWindow = c.status.SkippedOperationalLastWindow
	s.DatabaseBytes = size
	s.StoragePressure = size >= c.Config.Limits.MaxDatabaseBytes || free < c.Config.Limits.ReserveBytes
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
	sizeRuntime, err := TreeSize(c.Config.RuntimeDir)
	if err != nil {
		return errors.New("runtime_storage")
	}
	if sizeRuntime >= c.Config.Limits.MaxRuntimeBytes {
		return errors.New("runtime_limit")
	}

	size, err := c.Store.Size()
	if err != nil {
		return errors.New("storage_read")
	}
	if size >= c.Config.Limits.MaxDatabaseBytes {
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
func (c *Collector) rotate(ctx context.Context) error {
	// Stop before snapshotting: the old consumer must never see a partially rebuilt replay.
	previous := c.Consumer.Generation()
	if err := c.Consumer.Stop(ctx); err != nil {
		return err
	}
	if previous != "" && previous == c.Generation {
		// Published reports are independent snapshots; this known, reaped consumer no longer owns its input.
		if err := os.RemoveAll(filepath.Join(c.Config.RuntimeDir, "generations", previous)); err != nil {
			return errors.New("cleanup_storage")
		}
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
	if c.Consumer.NeedsReplay() {
		c.dirty = true
	}
	if c.dirty {
		return c.rotate(ctx)
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
		c.mu.Lock()
		c.status.ExportFailures++
		c.mu.Unlock()
		_ = c.Consumer.Stop(ctx)
		return errors.New("export_failed")
	}
	return nil
}
func (c *Collector) Window(ctx context.Context, start, end int64, advance bool) error {
	if err := ctx.Err(); err != nil {
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
	if err != nil {
		var q *source.QueryError
		if errors.As(err, &q) && q.Split && end-start >= 2*int64(config.Duration(c.Config.Collection.MinimumWindow)) {
			middle := start + (end-start)/2
			if err = c.Window(ctx, start, middle, advance); err != nil {
				return err
			}
			return c.Window(ctx, middle, end, advance)
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
		return err
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
	if err := c.Export(ctx); err != nil {
		return false, err
	}
	if err := c.Cleanup(); err != nil {
		return false, err
	}
	size := int64(config.Duration(c.Config.Collection.BackfillWindow))
	if cursor < target && (target-cursor > size || !now.Before(c.nextPoll)) {
		end := min(cursor+size, target)
		start := cursor
		if target-cursor <= size {
			start = max(cutoff, cursor-int64(config.Duration(c.Config.Collection.Overlap)))
		}
		if err := c.Window(ctx, start, end, true); err != nil {
			return false, err
		}
		c.nextPoll = now.Add(config.Duration(c.Config.Collection.PollInterval))
		c.failure = ""
		return target-end > size, nil
	}
	if c.reconcileEnd == 0 && !now.Before(c.timeReconcile) && cursor >= target-size {
		c.reconcileStart = cutoff
		c.reconcileEnd = min(cursor, target)
	}
	if c.reconcileEnd != 0 {
		end := min(c.reconcileStart+size, c.reconcileEnd)
		if c.reconcileStart < end {
			if err := c.Window(ctx, c.reconcileStart, end, false); err != nil {
				return false, err
			}
			c.reconcileStart = end
			c.failure = ""
			return true, nil
		}
		c.reconcileEnd = 0
		c.timeReconcile = now.Add(config.Duration(c.Config.Collection.ReconciliationInterval))
	}
	return false, nil
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
	_ = c.Consumer.Stop(shutdown)
	return c.Store.Close()
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
