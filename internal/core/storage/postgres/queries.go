package postgres

// SQL queries for event storage operations with principal tracking

const (
	// querySaveEvent inserts an event with principal idempotency.
	// Uses composite key (principal_id, id) to prevent duplicate events.
	// RETURNING clause retrieves auto-generated ingest_seq for cursor tracking.
	// ON CONFLICT DO NOTHING returns no rows (sql.ErrNoRows) for duplicates.
	querySaveEvent = `
		INSERT INTO events (
			id, principal_id, type, schema_version,
			occurred_at, ingested_at, metadata, data
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (principal_id, id) DO NOTHING
		RETURNING ingest_seq
	`

	// queryRetrieveEventsAfterCursor fetches events after a cursor (ingest_seq).
	// Used by recovery to replay events in strict total order.
	// Prevents batch boundary data loss by using monotonic sequence.
	// Note: Fetches events for ALL principals (sweeper processes globally).
	queryRetrieveEventsAfterCursor = `
		SELECT
			id, principal_id, type, schema_version,
			occurred_at, ingested_at, metadata, data, ingest_seq
		FROM events
		WHERE ingest_seq > $1
		ORDER BY ingest_seq ASC
		LIMIT $2
	`

	// queryRetrieveEventsAfter - DEPRECATED: Use queryRetrieveEventsAfterCursor
	// Kept for backwards compatibility during migration.
	queryRetrieveEventsAfter = `
		SELECT
			id, principal_id, type, schema_version,
			occurred_at, ingested_at, metadata, data, ingest_seq
		FROM events
		WHERE ingested_at > $1
		ORDER BY ingested_at ASC, ingest_seq ASC
		LIMIT $2
	`

	// queryRetrieveScopedEventsAfterCursor fetches unflushed events for one query scope.
	// Used by projection hybrid read path to merge pre-aggregates with tail raw events.
	queryRetrieveScopedEventsAfterCursor = `
		SELECT
			id, principal_id, type, schema_version,
			occurred_at, ingested_at, metadata, data, ingest_seq
		FROM events
		WHERE ingest_seq > $1
		  AND principal_id = $2
		  AND type = $3
		  AND occurred_at >= $4
		  AND occurred_at < $5
		ORDER BY ingest_seq ASC
		LIMIT $6
	`
)
