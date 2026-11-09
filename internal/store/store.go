// Package store maintains the original collector's authoritative SQLite format.
package store

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/zekihan/accessrelay/internal/telemetry"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const Schema = `
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (id INTEGER PRIMARY KEY, identity TEXT UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY, digest BLOB UNIQUE NOT NULL, time_ns INTEGER NOT NULL,
 source_id INTEGER REFERENCES sources(id), message TEXT NOT NULL, occurrences INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS events_time ON events(time_ns);
CREATE INDEX IF NOT EXISTS events_source ON events(source_id);
CREATE TABLE IF NOT EXISTS changes (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,copies INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS changes_event ON changes(event_id);`

type Store struct {
	DB      *sql.DB
	lock    *os.File
	Path    string
	options Options
}

type Options struct {
	MmapBytes int64
	// DeferIntegrity skips the blocking quick_check during Open. The caller
	// must run Verify before relying on the database being intact.
	DeferIntegrity bool
	// ReplayPrefetch is the number of query-only readers that warm the page
	// cache ahead of a full replay export. Zero disables prefetching.
	ReplayPrefetch int
	// Synchronous is "full" (default) or "normal". In WAL mode NORMAL keeps
	// commits atomic but may lose the most recent ones on power loss.
	Synchronous string
}

func Open(dir string) (*Store, error) {
	return OpenWithOptions(dir, Options{})
}

func OpenWithOptions(dir string, options Options) (s *Store, err error) {
	opening := telemetry.Start("database_open")
	defer func() { opening.Finish(err == nil) }()
	if options.MmapBytes < 0 || options.MmapBytes > 2<<30 || options.ReplayPrefetch < 0 || options.ReplayPrefetch > 64 {
		return nil, errors.New("configuration_database")
	}
	synchronous := "FULL"
	switch options.Synchronous {
	case "", "full":
	case "normal":
		synchronous = "NORMAL"
	default:
		return nil, errors.New("configuration_database")
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil, errors.New("state_storage")
	}
	lock, err := os.OpenFile(filepath.Join(dir, "collector.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.New("writer_lock")
	}
	if unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB) != nil {
		_ = lock.Close()
		return nil, errors.New("writer_in_use")
	}
	s = &Store{lock: lock, Path: filepath.Join(dir, "events.sqlite"), options: options}
	defer func() {
		if err != nil {
			_ = s.Close()
		}
	}()
	// database/sql may replace a failed connection (including after an
	// interrupted query). Connection-local settings must follow every open.
	dsn := url.URL{Scheme: "file", Path: s.Path}
	dsn.RawQuery = url.Values{"_pragma": {
		"foreign_keys(ON)", "journal_mode(WAL)", "synchronous(" + synchronous + ")",
		"busy_timeout(5000)", "journal_size_limit(8388608)",
		fmt.Sprintf("mmap_size(%d)", options.MmapBytes),
	}}.Encode()
	s.DB, err = sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, errors.New("database_open")
	}
	s.DB.SetMaxOpenConns(1)
	if err = s.DB.Ping(); err != nil {
		return s, errors.New("database_open")
	}
	var version int
	if err = s.DB.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 0 {
		return s, errors.New("database_format")
	}
	var tables int
	if err = s.DB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
		return s, errors.New("database_format")
	}
	if tables == 0 {
		if _, err = s.DB.Exec(Schema); err != nil {
			return s, errors.New("database_schema")
		}
	} else if tables != 4 {
		return s, errors.New("database_format")
	}
	// The inherited schema has user_version 0. No schema rewrite or digest conversion is performed.
	for table, columns := range map[string]string{"metadata": "key,value", "sources": "id,identity", "events": "id,digest,time_ns,source_id,message,occurrences", "changes": "seq,event_id,copies"} {
		rows, e := s.DB.Query("SELECT name FROM pragma_table_info(?) ORDER BY cid", table)
		if e != nil {
			return s, errors.New("database_schema")
		}
		names := ""
		for rows.Next() {
			var name string
			if e = rows.Scan(&name); e != nil {
				_ = rows.Close()
				return s, errors.New("database_schema")
			}
			if names != "" {
				names += ","
			}
			names += name
		}
		e = rows.Err()
		_ = rows.Close()
		if e != nil || names != columns {
			return s, errors.New("database_format")
		}
	}
	if options.DeferIntegrity {
		return s, nil
	}
	if err = quickCheck(context.Background(), s.DB); err != nil {
		return s, err
	}
	return s, nil
}

func quickCheck(ctx context.Context, db *sql.DB) (err error) {
	integrity := telemetry.Start("database_integrity")
	defer func() { integrity.Finish(err == nil) }()
	var quick string
	if e := db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&quick); e != nil {
		return storageError(ctx, "database_integrity")
	}
	if quick != "ok" {
		return errors.New("database_integrity")
	}
	return nil
}

// Verify runs quick_check on a separate query-only connection so the writer
// connection stays available. WAL readers see one committed snapshot and do
// not block the writer.
func (s *Store) Verify(ctx context.Context) error {
	dsn := url.URL{Scheme: "file", Path: s.Path}
	dsn.RawQuery = url.Values{"_pragma": {
		"query_only(ON)", "busy_timeout(5000)",
		fmt.Sprintf("mmap_size(%d)", s.options.MmapBytes),
	}}.Encode()
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return errors.New("database_integrity")
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1)
	return quickCheck(ctx, db)
}
func (s *Store) Close() error {
	var err error
	if s.DB != nil {
		err = s.DB.Close()
	}
	if s.lock != nil {
		_ = unix.Flock(int(s.lock.Fd()), unix.LOCK_UN)
		if e := s.lock.Close(); err == nil {
			err = e
		}
		s.lock = nil
	}
	return err
}
func (s *Store) Get(key string, value any) error {
	var raw string
	err := s.DB.QueryRow("SELECT value FROM metadata WHERE key=?", key).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return errors.New("database_read")
	}
	if json.Unmarshal([]byte(raw), value) != nil {
		return errors.New("database_metadata")
	}
	return nil
}
func put(tx *sql.Tx, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT OR REPLACE INTO metadata VALUES(?,?)", key, string(raw))
	return err
}

type Gap struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

const insertInitialChanges = `INSERT INTO changes(event_id,copies) SELECT e.id,e.occurrences FROM window.records r CROSS JOIN events e WHERE e.digest=r.digest AND NOT EXISTS(SELECT 1 FROM changes c WHERE c.event_id=e.id)`

func (s *Store) Gap(start, end string, cursor int64) error {
	var gaps []Gap
	if err := s.Get("gaps", &gaps); err != nil {
		return err
	}
	if len(gaps) > 19 {
		gaps = gaps[len(gaps)-19:]
	}
	gaps = append(gaps, Gap{start, end})
	tx, err := s.DB.Begin()
	if err != nil {
		return errors.New("database_write")
	}
	defer func() { _ = tx.Rollback() }()
	if put(tx, "gaps", gaps) != nil || put(tx, "cursor", cursor) != nil || tx.Commit() != nil {
		return errors.New("database_write")
	}
	return nil
}
func (s *Store) CommitWindow(ctx context.Context, stage string, end int64, advance bool, success float64) (err error) {
	if _, err = s.DB.ExecContext(ctx, "ATTACH DATABASE ? AS window", stage); err != nil {
		return errors.New("database_stage")
	}
	defer func() {
		if _, e := s.DB.Exec("DETACH DATABASE window"); e != nil && err == nil {
			err = errors.New("database_detach")
		}
	}()
	tx, e := s.DB.BeginTx(ctx, nil)
	if e != nil {
		return errors.New("database_transaction")
	}
	defer func() { _ = tx.Rollback() }()
	statements := []string{
		`INSERT OR IGNORE INTO sources(identity) SELECT DISTINCT source FROM window.records`,
		`INSERT INTO changes(event_id,copies) SELECT e.id,r.copies-e.occurrences FROM window.records r JOIN events e ON e.digest=r.digest WHERE r.copies>e.occurrences`,
		`UPDATE events SET occurrences=(SELECT copies FROM window.records r WHERE r.digest=events.digest) WHERE digest IN(SELECT digest FROM window.records) AND occurrences<(SELECT copies FROM window.records r WHERE r.digest=events.digest)`,
		`INSERT INTO events(digest,time_ns,source_id,message,occurrences) SELECT r.digest,r.time_ns,s.id,r.message,r.copies FROM window.records r JOIN sources s ON s.identity=r.source WHERE NOT EXISTS(SELECT 1 FROM events e WHERE e.digest=r.digest)`,
		insertInitialChanges,
	}
	for _, q := range statements {
		if _, e = tx.ExecContext(ctx, q); e != nil {
			return errors.New("database_transaction")
		}
	}
	if advance {
		var raw string
		var cursor int64
		e = tx.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='cursor'").Scan(&raw)
		if e != nil && e != sql.ErrNoRows {
			return errors.New("database_metadata")
		}
		if e == nil && json.Unmarshal([]byte(raw), &cursor) != nil {
			return errors.New("database_metadata")
		}
		if cursor > end {
			end = cursor
		}
		if e = put(tx, "cursor", end); e != nil {
			return errors.New("database_transaction")
		}
	}
	if put(tx, "last_success", success) != nil || tx.Commit() != nil {
		return errors.New("database_transaction")
	}
	return nil
}

const pruneBatchSize = 1000

// pruneAutocheckpoint lets the WAL grow to about 64 MiB during retention so a
// checkpoint writes each page dirtied by several batches once instead of once
// per batch. The default of 1000 pages is restored afterwards.
const pruneAutocheckpoint = 16384

func (s *Store) Prune(ctx context.Context, cutoff int64, progress func()) (removed int64, err error) {
	retention := telemetry.Start("retention")
	defer func() { retention.Finish(err == nil) }()
	deletion := telemetry.Start("retention_delete")
	defer func() { deletion.Finish(err == nil) }()
	if _, err = s.DB.ExecContext(ctx, fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", pruneAutocheckpoint)); err != nil {
		return 0, storageError(ctx, "database_transaction")
	}
	defer func() { _, _ = s.DB.Exec(`PRAGMA wal_autocheckpoint=1000`) }()
	for {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return removed, storageError(ctx, "database_transaction")
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM events WHERE id IN (
			SELECT id FROM events WHERE time_ns<? ORDER BY time_ns,id LIMIT ?
		)`, cutoff, pruneBatchSize)
		if err != nil {
			_ = tx.Rollback()
			return removed, storageError(ctx, "database_transaction")
		}
		n, err := result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return removed, errors.New("database_transaction")
		}
		if err = tx.Commit(); err != nil {
			return removed, storageError(ctx, "database_transaction")
		}
		removed += n
		deletion.Progress(removed)
		if progress != nil {
			progress()
		}
		if n < pruneBatchSize {
			break
		}
	}
	deletion.Finish(true)
	cleanup := telemetry.Start("source_cleanup")
	_, err = s.DB.ExecContext(ctx, `DELETE FROM sources WHERE id NOT IN(SELECT source_id FROM events)`)
	cleanup.Finish(err == nil)
	if err != nil {
		return removed, storageError(ctx, "database_transaction")
	}
	checkpoint := telemetry.Start("retention_checkpoint")
	_, err = s.DB.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	checkpoint.Finish(err == nil)
	if err != nil {
		return removed, storageError(ctx, "database_checkpoint")
	}
	return removed, nil
}

func storageError(ctx context.Context, code string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New(code)
}

func (s *Store) Counts() (int64, int64, error) {
	var events, copies int64
	err := s.DB.QueryRow(`SELECT COUNT(*),COALESCE(SUM(occurrences),0) FROM events`).Scan(&events, &copies)
	return events, copies, err
}
func (s *Store) Latest() (int64, error) {
	var seq int64
	err := s.DB.QueryRow(`SELECT COALESCE(MAX(seq),0) FROM changes`).Scan(&seq)
	return seq, err
}
func (s *Store) Export(ctx context.Context, output io.Writer, after int64, full bool, maxBytes int64, progress func()) (latest int64, err error) {
	var replay *telemetry.Phase
	if full {
		replay = telemetry.Start("replay_export")
	}
	var written, lines int64
	defer func() {
		replay.Progress(lines)
		replay.Finish(err == nil)
	}()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, storageError(ctx, "export_database")
	}
	defer func() { _ = tx.Rollback() }()
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM changes`).Scan(&latest); err != nil {
		return 0, storageError(ctx, "export_database")
	}
	var rows *sql.Rows
	var prefetch *replayPrefetch
	if full {
		prefetch = s.startReplayPrefetch(ctx, s.options.ReplayPrefetch)
		defer prefetch.stop()
		rows, err = tx.QueryContext(ctx, `SELECT message,occurrences,time_ns FROM events ORDER BY time_ns,id`)
	} else {
		rows, err = tx.QueryContext(ctx, `SELECT e.message,c.copies,0 FROM changes c JOIN events e ON c.event_id=e.id WHERE c.seq>? ORDER BY c.seq`, after)
	}
	if err != nil {
		return 0, storageError(ctx, "export_database")
	}
	defer func() { _ = rows.Close() }()
	buffer := bufio.NewWriterSize(output, 256*1024)
	for rows.Next() {
		var message string
		var copies, timeNS int64
		if rows.Scan(&message, &copies, &timeNS) != nil || copies < 1 {
			return 0, storageError(ctx, "export_database")
		}
		prefetch.advance(timeNS)
		line := message + "\n"
		if copies > (maxBytes-written)/int64(len(line)) {
			return 0, errors.New("replay_limit")
		}
		for range copies {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if _, err = buffer.WriteString(line); err != nil {
				return 0, errors.New("export_write")
			}
			written += int64(len(line))
			lines++
			if lines%1000 == 0 {
				replay.Progress(lines)
				if progress != nil {
					progress()
				}
			}
		}
	}
	if rows.Err() != nil {
		return 0, storageError(ctx, "export_database")
	}
	_ = rows.Close()
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if err = buffer.Flush(); err != nil {
		return 0, storageError(ctx, "export_write")
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if tx.Commit() != nil {
		return 0, storageError(ctx, "export_database")
	}
	return latest, nil
}
func (s *Store) Size() (int64, error) {
	var n int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		f, err := os.Stat(s.Path + suffix)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return n, fmt.Errorf("database_storage")
		}
		n += f.Size()
	}
	return n, nil
}
