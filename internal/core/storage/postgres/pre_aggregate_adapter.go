package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/shopspring/decimal"
)

const (
	defaultBucketSize = "1m"

	querySelectCheckpointForUpdate = `
		SELECT checkpoint_cursor
		FROM sweep_checkpoints
		WHERE bucket_size = $1
		FOR UPDATE
	`

	queryInitCheckpointRow = `
		INSERT INTO sweep_checkpoints (bucket_size, checkpoint_cursor, updated_at)
		VALUES ($1, 0, $2)
		ON CONFLICT (bucket_size) DO NOTHING
	`

	queryUpsertPreAggregate = `
		INSERT INTO pre_aggregates (
			partition_id, principal_id, rule_name, rule_fingerprint,
			bucket_size, window_start, operator, value, event_count, last_event_id, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (partition_id, principal_id, rule_name, bucket_size, window_start)
		DO UPDATE SET
			value = CASE EXCLUDED.operator
				WHEN 'count' THEN pre_aggregates.value + EXCLUDED.value
				WHEN 'sum' THEN pre_aggregates.value + EXCLUDED.value
				WHEN 'min' THEN LEAST(pre_aggregates.value, EXCLUDED.value)
				WHEN 'max' THEN GREATEST(pre_aggregates.value, EXCLUDED.value)
				ELSE EXCLUDED.value
			END,
			event_count      = pre_aggregates.event_count + EXCLUDED.event_count,
			last_event_id    = EXCLUDED.last_event_id,
			rule_fingerprint = EXCLUDED.rule_fingerprint,
			updated_at       = EXCLUDED.updated_at
	`

	queryUpdateCheckpoint = `
		UPDATE sweep_checkpoints
		SET checkpoint_cursor = $1, updated_at = $2
		WHERE bucket_size = $3
	`

	queryReadCheckpoint = `SELECT checkpoint_cursor FROM sweep_checkpoints WHERE bucket_size = $1`

	queryLoadAggregates = `
		SELECT
			partition_id, principal_id, rule_name, rule_fingerprint,
			bucket_size, window_start, operator, value, event_count, last_event_id, updated_at
		FROM pre_aggregates
	`

	queryRangePreAggregates = `
		SELECT
			window_start,
			operator,
			value,
			event_count,
			last_event_id,
			rule_fingerprint,
			updated_at
		FROM pre_aggregates
		WHERE partition_id = $1
		  AND principal_id = $2
		  AND rule_name = $3
		  AND bucket_size = $4
		  AND window_start >= $5
		  AND window_start < $6
		ORDER BY window_start ASC
	`

	queryRangePreAggregatesWithCheckpoint = `
		WITH checkpoint AS (
			SELECT COALESCE(
				(SELECT checkpoint_cursor FROM sweep_checkpoints WHERE bucket_size = $4),
				0
			) AS checkpoint_cursor
		),
		scoped AS (
			SELECT
				window_start,
				operator,
				value,
				event_count,
				last_event_id,
				rule_fingerprint,
				updated_at
			FROM pre_aggregates
			WHERE partition_id = $1
			  AND principal_id = $2
			  AND rule_name = $3
			  AND bucket_size = $4
			  AND window_start >= $5
			  AND window_start < $6
		)
		SELECT
			checkpoint.checkpoint_cursor,
			scoped.window_start,
			scoped.operator,
			scoped.value,
			scoped.event_count,
			scoped.last_event_id,
			scoped.rule_fingerprint,
			scoped.updated_at
		FROM checkpoint
		LEFT JOIN scoped ON TRUE
		ORDER BY scoped.window_start ASC NULLS LAST
	`
)

// PreAggregateAdapter implements aggregation.PreAggregateStore using PostgreSQL.
// Flush and checkpoint writes are in a single transaction — the atomicity
// contract that makes crash recovery safe.
type PreAggregateAdapter struct {
	db *sql.DB
}

// NewPreAggregateAdapter creates a new PreAggregateAdapter sharing the given connection.
func NewPreAggregateAdapter(db *sql.DB) *PreAggregateAdapter {
	return &PreAggregateAdapter{db: db}
}

// Flush upserts all pre-aggregates and writes bucket-scoped checkpoint cursor in one transaction.
// cursor is the last ingest_seq included in this state snapshot.
func (a *PreAggregateAdapter) Flush(
	ctx context.Context,
	aggregates map[aggregation.AggregateKey]aggregation.AggregateState,
	cursor int64,
	bucketSize string,
) error {
	if bucketSize == "" {
		bucketSize = defaultBucketSize
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pre_aggregate flush: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Lock checkpoint row first and enforce monotonic checkpoint writes.
	// This prevents stale, out-of-order flushes from overwriting newer durable state.
	var durableCursor int64
	err = tx.QueryRowContext(ctx, querySelectCheckpointForUpdate, bucketSize).Scan(&durableCursor)
	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, queryInitCheckpointRow, bucketSize, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("pre_aggregate flush: init checkpoint row: %w", err)
		}

		err = tx.QueryRowContext(ctx, querySelectCheckpointForUpdate, bucketSize).Scan(&durableCursor)
		if err != nil {
			return fmt.Errorf("pre_aggregate flush: read initialized checkpoint for update: %w", err)
		}
	}
	if err != nil {
		return fmt.Errorf("pre_aggregate flush: read checkpoint for update: %w", err)
	}

	if cursor <= durableCursor {
		slog.Warn("[PreAggregateAdapter] Skipping stale/no-op flush",
			"cursor", cursor,
			"durable_cursor", durableCursor,
			"aggregates", len(aggregates))
		return nil
	}

	upsertStmt, err := tx.PrepareContext(ctx, queryUpsertPreAggregate)
	if err != nil {
		return fmt.Errorf("pre_aggregate flush: prepare upsert: %w", err)
	}
	defer upsertStmt.Close()

	for key, state := range aggregates {
		keyBucketSize := key.BucketSize
		if keyBucketSize == "" {
			keyBucketSize = defaultBucketSize
		}
		if keyBucketSize != bucketSize {
			return fmt.Errorf(
				"pre_aggregate flush: aggregate bucket mismatch: expected %s, got %s for key %v",
				bucketSize,
				keyBucketSize,
				key,
			)
		}
		if _, err := upsertStmt.ExecContext(ctx,
			key.PartitionID,
			key.PrincipalID,
			key.RuleName,
			state.RuleFingerprint,
			keyBucketSize,
			key.WindowStart,
			state.Operator,
			state.Value,
			state.EventCount,
			state.LastEventID,
			state.UpdatedAt,
		); err != nil {
			return fmt.Errorf("pre_aggregate flush: upsert %v: %w", key, err)
		}
	}

	// Write single global checkpoint — same transaction as the upserts.
	// No partition tracking needed for single-instance deployment.
	result, err := tx.ExecContext(ctx, queryUpdateCheckpoint, cursor, time.Now().UTC(), bucketSize)
	if err != nil {
		return fmt.Errorf("pre_aggregate flush: write checkpoint: %w", err)
	}

	// Verify singleton row was updated
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("pre_aggregate flush: check checkpoint write: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("pre_aggregate flush: checkpoint row missing (bucket=%s)", bucketSize)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pre_aggregate flush: commit: %w", err)
	}

	slog.Info("[PreAggregateAdapter] Flushed",
		"aggregates", len(aggregates),
		"cursor", cursor,
		"bucket_size", bucketSize,
	)
	return nil
}

// ReadCheckpoint returns the bucket-scoped checkpoint cursor.
// Returns 0 if no checkpoint exists yet (meaning "replay from beginning").
func (a *PreAggregateAdapter) ReadCheckpoint(ctx context.Context, bucketSize string) (int64, error) {
	if bucketSize == "" {
		bucketSize = defaultBucketSize
	}

	var cursor int64
	err := a.db.QueryRowContext(ctx,
		queryReadCheckpoint,
		bucketSize,
	).Scan(&cursor)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read global checkpoint: %w", err)
	}
	return cursor, nil
}

// LoadAggregates loads all durable pre-aggregates from the database.
// Used during recovery to bootstrap StateMap before replaying delta events.
// This prevents overwriting correct historical totals with partial sums.
func (a *PreAggregateAdapter) LoadAggregates(ctx context.Context) (map[aggregation.AggregateKey]aggregation.AggregateState, error) {
	rows, err := a.db.QueryContext(ctx, queryLoadAggregates)
	if err != nil {
		return nil, fmt.Errorf("load aggregates: %w", err)
	}
	defer rows.Close()

	aggregates := make(map[aggregation.AggregateKey]aggregation.AggregateState)
	var count int

	for rows.Next() {
		var key aggregation.AggregateKey
		var state aggregation.AggregateState
		var valueStr string

		if err := rows.Scan(
			&key.PartitionID,
			&key.PrincipalID,
			&key.RuleName,
			&state.RuleFingerprint,
			&key.BucketSize,
			&key.WindowStart,
			&state.Operator,
			&valueStr,
			&state.EventCount,
			&state.LastEventID,
			&state.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("load aggregates: scan row: %w", err)
		}

		value, err := decimal.NewFromString(valueStr)
		if err != nil {
			return nil, fmt.Errorf("load aggregates: parse value %q: %w", valueStr, err)
		}
		state.Value = value
		state.WindowStart = key.WindowStart

		aggregates[key] = state
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load aggregates: iterate rows: %w", err)
	}

	slog.Info("[PreAggregateAdapter] Loaded aggregates from database", "count", count)
	return aggregates, nil
}

// QueryRange fetches pre-aggregates for a time range.
// Used by projection API to serve usage queries.
// Returns aggregates ordered by window_start ASC.
func (a *PreAggregateAdapter) QueryRange(
	ctx context.Context,
	principalID string,
	ruleName string,
	bucketSize string,
	startTime time.Time,
	endTime time.Time,
) ([]aggregation.AggregateState, error) {
	if bucketSize == "" {
		bucketSize = defaultBucketSize
	}

	// Use fixed partition_id=0 for single-tenant deployments
	partitionID := 0

	rows, err := a.db.QueryContext(ctx, queryRangePreAggregates, partitionID, principalID, ruleName, bucketSize, startTime, endTime)

	if err != nil {
		return nil, fmt.Errorf("query pre_aggregates: %w", err)
	}
	defer rows.Close()

	var results []aggregation.AggregateState
	for rows.Next() {
		var state aggregation.AggregateState
		var valueStr string

		err := rows.Scan(
			&state.WindowStart,
			&state.Operator,
			&valueStr,
			&state.EventCount,
			&state.LastEventID,
			&state.RuleFingerprint,
			&state.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		value, err := decimal.NewFromString(valueStr)
		if err != nil {
			return nil, fmt.Errorf("parse value %q: %w", valueStr, err)
		}
		state.Value = value

		results = append(results, state)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return results, nil
}

// QueryRangeWithCheckpoint fetches pre-aggregates and the bucket checkpoint from one
// statement snapshot. This prevents races where checkpoint and aggregates are read
// from different flush versions.
func (a *PreAggregateAdapter) QueryRangeWithCheckpoint(
	ctx context.Context,
	principalID string,
	ruleName string,
	bucketSize string,
	startTime time.Time,
	endTime time.Time,
) ([]aggregation.AggregateState, int64, error) {
	if bucketSize == "" {
		bucketSize = defaultBucketSize
	}

	// Use fixed partition_id=0 for single-tenant deployments
	partitionID := 0

	rows, err := a.db.QueryContext(
		ctx,
		queryRangePreAggregatesWithCheckpoint,
		partitionID, principalID, ruleName, bucketSize, startTime, endTime,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query pre_aggregates with checkpoint: %w", err)
	}
	defer rows.Close()

	var (
		results           []aggregation.AggregateState
		checkpoint        int64
		checkpointScanned bool
	)

	for rows.Next() {
		var (
			scannedCheckpoint int64
			windowStart       sql.NullTime
			operator          sql.NullString
			valueStr          sql.NullString
			eventCount        sql.NullInt64
			lastEventID       sql.NullString
			ruleFingerprint   sql.NullString
			updatedAt         sql.NullTime
		)

		if err := rows.Scan(
			&scannedCheckpoint,
			&windowStart,
			&operator,
			&valueStr,
			&eventCount,
			&lastEventID,
			&ruleFingerprint,
			&updatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan row: %w", err)
		}

		if !checkpointScanned {
			checkpoint = scannedCheckpoint
			checkpointScanned = true
		}

		// LEFT JOIN emits one row with NULL aggregate columns when range is empty.
		if !windowStart.Valid {
			continue
		}
		if !valueStr.Valid {
			return nil, 0, fmt.Errorf("scan row: aggregate value is NULL")
		}

		value, parseErr := decimal.NewFromString(valueStr.String)
		if parseErr != nil {
			return nil, 0, fmt.Errorf("parse value %q: %w", valueStr.String, parseErr)
		}

		results = append(results, aggregation.AggregateState{
			WindowStart:     windowStart.Time,
			Operator:        operator.String,
			Value:           value,
			EventCount:      eventCount.Int64,
			LastEventID:     lastEventID.String,
			RuleFingerprint: ruleFingerprint.String,
			UpdatedAt:       updatedAt.Time,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate rows: %w", err)
	}

	return results, checkpoint, nil
}
