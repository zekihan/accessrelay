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
	return s.PutContext(context.Background(), key, value)
}

// PutContext stores a metadata value in its own transaction with the caller context.
func (s *Store) PutContext(ctx context.Context, key string, value any) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("database_transaction")
	}
	defer func() { _ = tx.Rollback() }()
	if put(ctx, tx, key, value) != nil || tx.Commit() != nil {
		return errors.New("database_transaction")
	}
	return nil
}

// PurgeEvents deletes every event whose source identity and message match, together with its
// change records, in transactions of at most pruneBatchSize events. Committed
// batches are final, so an interrupted purge resumes by scanning again. Once
// every event has been checked, key is set to value in the same transaction
// that removes sources left without events.
func (s *Store) PurgeEvents(
	ctx context.Context,
	match func(identity, message string) bool,
	key string,
	value any,
	progress func(),
) (removed int64, err error) {
	purge := telemetry.Start("exclusion_purge")
	defer func() { purge.Finish(err == nil) }()
	if _, err = s.DB.ExecContext(
		ctx,
		fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", pruneAutocheckpoint),
	); err != nil {
		return 0, storageError(ctx, "database_transaction")
	}
	defer func() {
		if _, restoreErr := s.DB.ExecContext(
			ctx,
			`PRAGMA wal_autocheckpoint=1000`,
		); restoreErr != nil {
			err = storageError(ctx, "database_transaction")
		}
	}()
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
	if _, err = tx.ExecContext(ctx, deleteUnusedSources); err != nil {
		return removed, storageError(ctx, "database_transaction")
	}
	if err = put(ctx, tx, key, value); err != nil {
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

func (s *Store) matchingEvents(
	ctx context.Context,
	after int64,
	match func(identity, message string) bool,
) (ids []any, last int64, scanned int, err error) {
	rows, err := s.DB.QueryContext(
		ctx,
		`SELECT e.id,COALESCE(s.identity,''),e.message FROM events e
		LEFT JOIN sources s ON s.id=e.source_id WHERE e.id>? ORDER BY e.id LIMIT ?`,
		after,
		purgeScanBatch,
	)
	if err != nil {
		return nil, 0, 0, storageError(ctx, "database_read")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var identity, message string
		if err = rows.Scan(&last, &identity, &message); err != nil {
			return nil, 0, 0, storageError(ctx, "database_read")
		}
		scanned++
		if match(identity, message) {
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
	// #nosec G202 -- the concatenation only repeats the "?" placeholder.
	query := `DELETE FROM events WHERE id IN(?` + strings.Repeat(",?", len(ids)-1) + `)`
	if _, err = tx.ExecContext(ctx, query, ids...); err != nil {
		return storageError(ctx, "database_transaction")
	}
	if err = tx.Commit(); err != nil {
		return storageError(ctx, "database_transaction")
	}
	return nil
}
