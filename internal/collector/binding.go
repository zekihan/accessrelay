package collector

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"

	"github.com/zekihan/accessrelay/internal/config"
)

func bindSources(db *sql.DB, collection config.Collection) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	metadataErr := errors.New("database_metadata")
	if err != nil {
		return metadataErr
	}
	defer func() { _ = tx.Rollback() }()

	// Leave the old key available to readers that predate multiple sources.
	var legacy config.Source
	hadLegacy, err := readBinding(ctx, tx, "source_identity", &legacy)
	if err != nil || (hadLegacy && !legacy.Valid()) {
		return metadataErr
	}
	var bound []config.Input
	hadBindings, err := readBinding(ctx, tx, "source_bindings", &bound)
	if err != nil || (hadBindings && len(bound) == 0) {
		return metadataErr
	}
	known := make(map[config.Source]string, len(bound))
	for _, input := range bound {
		if !input.Valid() || known[input.Source] != "" ||
			(input.Format != config.TraefikCLF && input.Format != config.GatehouseJSON) {
			return metadataErr
		}
		known[input.Source] = input.Format
	}

	if hadLegacy && !hadBindings {
		known[legacy] = config.TraefikCLF
		bound = append(bound, config.Input{Source: legacy, Format: config.TraefikCLF})
	}
	if hadLegacy && known[legacy] != config.TraefikCLF {
		return metadataErr
	}

	inputs := collection.Inputs()
	// Extend an established binding by retaining at least one of its inputs.
	// Keep retired bindings so that a rollback can resume either known input.
	if len(bound) > 0 && !slices.ContainsFunc(inputs, func(input config.Input) bool {
		return known[input.Source] == input.Format
	}) {
		return errors.New("source_mismatch")
	}
	for _, input := range inputs {
		format := known[input.Source]
		if format == "" {
			bound = append(bound, input)
		} else if format != input.Format {
			return errors.New("source_mismatch")
		}
	}
	if !hadLegacy && !hadBindings && collection.Sources == nil {
		if err := writeBinding(ctx, tx, "source_identity", collection.Source); err != nil {
			return metadataErr
		}
	}
	if collection.Sources != nil || hadBindings {
		if err := writeBinding(ctx, tx, "source_bindings", bound); err != nil {
			return metadataErr
		}
	}
	if tx.Commit() != nil {
		return metadataErr
	}
	return nil
}

func readBinding(ctx context.Context, tx *sql.Tx, key string, dest any) (bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key=?", key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal([]byte(raw), dest)
}

func writeBinding(ctx context.Context, tx *sql.Tx, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE value<>excluded.value`, key, string(raw))
	return err
}
