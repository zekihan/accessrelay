package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

const (
	prefetchChunks    = 4096
	prefetchLeadBytes = 128 << 20
)

// replayPrefetch warms the OS page cache ahead of a full replay export.
// Retention and reconciliation leave event pages scattered across the file, so
// the time-ordered scan otherwise issues one small random read at a time.
// Separate query-only connections read upcoming time ranges concurrently and
// discard the results; the export itself still reads every row from its own
// snapshot, so prefetching never affects output. Failures are ignored.
type replayPrefetch struct {
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	db       *sql.DB
	position atomic.Int64
}

func (s *Store) startReplayPrefetch(ctx context.Context, readers int) *replayPrefetch {
	if readers <= 0 {
		return nil
	}
	dsn := url.URL{Scheme: "file", Path: s.Path}
	dsn.RawQuery = url.Values{"_pragma": {
		"query_only(ON)", "busy_timeout(5000)",
		fmt.Sprintf("mmap_size(%d)", s.options.MmapBytes),
	}}.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil
	}
	db.SetMaxOpenConns(readers)
	var first, last sql.NullInt64
	var pages, pageSize int64
	// Separate MIN and MAX queries each use a single index descent; combined,
	// SQLite scans the whole index.
	if db.QueryRowContext(ctx, `SELECT MIN(time_ns) FROM events`).Scan(&first) != nil || !first.Valid ||
		db.QueryRowContext(ctx, `SELECT MAX(time_ns) FROM events`).Scan(&last) != nil || !last.Valid ||
		db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages) != nil ||
		db.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize) != nil {
		_ = db.Close()
		return nil
	}
	span := (last.Int64-first.Int64)/prefetchChunks + 1
	lead := max(int64(2*readers), prefetchLeadBytes*prefetchChunks/max(1, pages*pageSize))
	ctx, cancel := context.WithCancel(ctx)
	p := &replayPrefetch{cancel: cancel, db: db}
	p.position.Store(first.Int64)
	var next atomic.Int64
	for range readers {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				chunk := next.Add(1) - 1
				if chunk >= prefetchChunks {
					return
				}
				start := first.Int64 + chunk*span
				for (start-p.position.Load())/span >= lead {
					select {
					case <-ctx.Done():
						return
					case <-time.After(2 * time.Millisecond):
					}
				}
				if ctx.Err() != nil || p.read(ctx, start, start+span) != nil {
					return
				}
			}
		}()
	}
	return p
}

func (p *replayPrefetch) read(ctx context.Context, start, end int64) error {
	rows, err := p.db.QueryContext(ctx, `SELECT message FROM events WHERE time_ns>=? AND time_ns<?`, start, end)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var message sql.RawBytes
		if err = rows.Scan(&message); err != nil {
			return err
		}
	}
	return rows.Err()
}

// advance records the export's current position. It is safe on a nil receiver.
func (p *replayPrefetch) advance(timeNS int64) {
	if p != nil {
		p.position.Store(timeNS)
	}
}

func (p *replayPrefetch) stop() {
	if p == nil {
		return
	}
	p.cancel()
	p.wg.Wait()
	_ = p.db.Close()
}
