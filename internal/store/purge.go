package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/zekihan/accessrelay/internal/telemetry"
)

const purgeScanBatch = 10000

// Put stores a metadata value in its own transaction.
func (s *Store) Put(key string, value any) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return errors.New("database_transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if put(tx, key, value) != nil || tx.Commit() != nil {
		return errors.New("database_transaction")
	}
	return nil
}

// PurgeEvents deletes every event whose message matches, together with its
// change records, in transactions of at most pruneBatchSize events. Committed
// batches are final, so an interrupted purge resumes by scanning again. Once
// every event has been checked, key is set to value in the same transaction
// that removes sources left without events.
func (s *Store) PurgeEvents(ctx context.Context, match func(string) bool, key string, value any, progress func()) (removed int64, err error) {
	purge := telemetry.Start("exclusion_purge")
	defer func() { purge.Finish(err == nil) }()
	if _, err = s.DB.ExecContext(ctx, fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", pruneAutocheckpoint)); err != nil {
		return 0, storageError(ctx, "database_transaction")
	}
	defer func() { _, _ = s.DB.Exec(`PRAGMA wal_autocheckpoint=1000`) }()
	var after int64
	for {
		ids, last, scanned, err := s.matchingEvents(ctx, after, match)
		if err != nil {
			return removed, err
		}
		for len(ids) > 0 {
			n := min(len(ids), pruneBatchSize)
			if err = s.deleteEvents(ctx, ids[:n]); err != nil {
				return removed, err
			}
			removed += int64(n)
			ids = ids[n:]
			purge.Progress(removed)
			if progress != nil {
				progress()
			}
		}
		if scanned < purgeScanBatch {
			break
		}
		after = last
		if progress != nil {
			progress()
		}
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return removed, storageError(ctx, "database_transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `DELETE FROM sources WHERE id NOT IN(SELECT source_id FROM events)`); err != nil {
		return removed, storageError(ctx, "database_transaction")
	}
	if err = put(tx, key, value); err != nil {
		return removed, storageError(ctx, "database_transaction")
	}
	if err = tx.Commit(); err != nil {
		return removed, storageError(ctx, "database_transaction")
	}
	if _, err = s.DB.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return removed, storageError(ctx, "database_checkpoint")
	}
	return removed, nil
}

func (s *Store) matchingEvents(ctx context.Context, after int64, match func(string) bool) (ids []any, last int64, scanned int, err error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id,message FROM events WHERE id>? ORDER BY id LIMIT ?`, after, purgeScanBatch)
	if err != nil {
		return nil, 0, 0, storageError(ctx, "database_read")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var message string
		if err = rows.Scan(&last, &message); err != nil {
			return nil, 0, 0, storageError(ctx, "database_read")
		}
		scanned++
		if match(message) {
			ids = append(ids, last)
		}
	}
	if rows.Err() != nil {
		return nil, 0, 0, storageError(ctx, "database_read")
	}
	return ids, last, scanned, nil
}

func (s *Store) deleteEvents(ctx context.Context, ids []any) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return storageError(ctx, "database_transaction")
	}
	defer func() { _ = tx.Rollback() }()
	query := `DELETE FROM events WHERE id IN(?` + strings.Repeat(",?", len(ids)-1) + `)`
	if _, err = tx.ExecContext(ctx, query, ids...); err != nil {
		return storageError(ctx, "database_transaction")
	}
	if err = tx.Commit(); err != nil {
		return storageError(ctx, "database_transaction")
	}
	return nil
}
