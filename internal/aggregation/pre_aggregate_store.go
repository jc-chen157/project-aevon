package aggregation

import (
	"context"
	"time"

	"github.com/aevon-lab/project-aevon/internal/core/aggregation"
)

// PreAggregateStore is the interface for durable pre-aggregate persistence.
// The sweeper flushes in-memory state through this interface.
//
// Contract: Flush and checkpoint write are atomically linked — a single
// database transaction. This prevents the crash scenario where flush succeeds
// but the checkpoint is not written, which would cause double-counting on replay.
//
// Checkpoint Invariant: "Checkpoint cursor N means: state includes all events
// up to ingest_seq N, and none after."
//
// Checkpoints are tracked by bucket_size so each aggregation bucket can run independently.
type PreAggregateStore interface {
	// Flush upserts all aggregates and writes the bucket-scoped checkpoint atomically.
	// cursor is the last ingest_seq included in this state snapshot.
	// Both aggregates and cursor are written in a single database transaction.
	Flush(
		ctx context.Context,
		aggregates map[aggregation.AggregateKey]aggregation.AggregateState,
		cursor int64,
		bucketSize string,
	) error

	// ReadCheckpoint returns the bucket-scoped checkpoint cursor.
	// Returns 0 if no checkpoint exists yet (meaning "replay from beginning").
	ReadCheckpoint(ctx context.Context, bucketSize string) (int64, error)

	// LoadAggregates loads all durable pre-aggregates from the database.
	// Used during recovery to bootstrap StateMap before replaying delta events.
	// This ensures we don't overwrite correct historical totals with partial sums.
	LoadAggregates(ctx context.Context) (map[aggregation.AggregateKey]aggregation.AggregateState, error)

	// QueryRange fetches pre-aggregates for a time range.
	// Used by projection API to serve usage queries.
	// Returns aggregates ordered by window_start ASC.
	// Filters by principal, rule, and time range.
	QueryRange(
		ctx context.Context,
		principalID string,
		ruleName string,
		bucketSize string,
		startTime time.Time,
		endTime time.Time,
	) ([]aggregation.AggregateState, error)
}
