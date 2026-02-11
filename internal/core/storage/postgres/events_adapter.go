package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	"github.com/aevon-lab/project-aevon/internal/core/storage"
	_ "github.com/lib/pq" // Register postgres driver
)

const connectPingTimeout = 5 * time.Second

// Adapter implements storage.EventStore for PostgreSQL.
type Adapter struct {
	db                       *sql.DB
	stmtSaveEvent            *sql.Stmt
	stmtRetrieveEvents       *sql.Stmt
	stmtRetrieveEventsCursor *sql.Stmt
	stmtRetrieveScopedCursor *sql.Stmt
}

// NewAdapter creates a new PostgreSQL storage adapter.
// Expects a valid PostgreSQL DSN (connection string) and connection pool settings.
//
// Example DSN: "postgres://user:password@localhost:5432/dbname?sslmode=disable"
//
// IMPORTANT: Schema must be initialized separately via migrations.
// Run migrations/001_create_events_table.up.sql before starting the application.
//
// The adapter prepares statements during initialization for performance.
func NewAdapter(dsn string, maxOpenConns, maxIdleConns int) (*Adapter, error) {

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	// Apply connection pool settings from config
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(5 * time.Minute)

	slog.Info("[Postgres] Connection pool configured",
		"max_open_conns", maxOpenConns,
		"max_idle_conns", maxIdleConns)

	pingCtx, cancel := context.WithTimeout(context.Background(), connectPingTimeout)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	if err := validateSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema validation failed - did you run migrations?: %w", err)
	}

	stmtSave, err := db.Prepare(querySaveEvent)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to prepare saveEvent statement: %w", err)
	}

	stmtRetrieve, err := db.Prepare(queryRetrieveEventsAfter)
	if err != nil {
		stmtSave.Close()
		db.Close()
		return nil, fmt.Errorf("failed to prepare retrieveEventsAfter statement: %w", err)
	}

	stmtRetrieveCursor, err := db.Prepare(queryRetrieveEventsAfterCursor)
	if err != nil {
		stmtSave.Close()
		stmtRetrieve.Close()
		db.Close()
		return nil, fmt.Errorf("failed to prepare retrieveEventsAfterCursor statement: %w", err)
	}

	stmtRetrieveScopedCursor, err := db.Prepare(queryRetrieveScopedEventsAfterCursor)
	if err != nil {
		stmtSave.Close()
		stmtRetrieve.Close()
		stmtRetrieveCursor.Close()
		db.Close()
		return nil, fmt.Errorf("failed to prepare retrieveScopedEventsAfterCursor statement: %w", err)
	}

	slog.Info("[Postgres] Adapter initialized with prepared statements")

	return &Adapter{
		db:                       db,
		stmtSaveEvent:            stmtSave,
		stmtRetrieveEvents:       stmtRetrieve,
		stmtRetrieveEventsCursor: stmtRetrieveCursor,
		stmtRetrieveScopedCursor: stmtRetrieveScopedCursor,
	}, nil
}

// validateSchema checks if the events table exists.
// Returns an error if the table is missing (migrations not run).
func validateSchema(db *sql.DB) error {
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_name = 'events'
		)
	`
	err := db.QueryRow(query).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check schema: %w", err)
	}
	if !exists {
		return fmt.Errorf("events table does not exist")
	}
	return nil
}

// SaveEvent persists an event to PostgreSQL and populates IngestSeq.
// Uses composite key (tenant_id, principal_id, id) for idempotency.
// Returns storage.ErrDuplicate if an event with the same key already exists.
// IMPORTANT: Populates event.IngestSeq from database for cursor tracking.
func (a *Adapter) SaveEvent(ctx context.Context, event *v1.Event) error {
	metadataJSON, dataJSON, err := marshalEventJSON(event)
	if err != nil {
		return err
	}

	// Use QueryRowContext to retrieve RETURNING ingest_seq
	var ingestSeq int64
	err = a.stmtSaveEvent.QueryRowContext(ctx,
		event.ID,
		event.TenantID,
		event.PrincipalID,
		event.Type,
		event.SchemaVersion,
		event.OccurredAt,
		event.IngestedAt,
		metadataJSON,
		dataJSON,
	).Scan(&ingestSeq)

	if err == sql.ErrNoRows {
		// ON CONFLICT DO NOTHING - event already exists (duplicate)
		return storage.ErrDuplicate
	}
	if err != nil {
		return fmt.Errorf("failed to save event: %w", err)
	}

	// Populate IngestSeq so it flows through the aggregation pipeline correctly
	event.IngestSeq = ingestSeq

	slog.Debug("[Postgres] Saved event",
		"tenant_id", event.TenantID,
		"principal_id", event.PrincipalID,
		"event_id", event.ID,
		"ingest_seq", ingestSeq)
	return nil
}

// RetrieveEventsAfter fetches events ingested after a given timestamp.
// Returns events ordered by ingested_at ASC (chronological).
// Used by the aggregation sweeper to process events in batches.
//
// Parameters:
//   - afterTime: Fetch events with ingested_at > afterTime
//   - limit: Maximum number of events to return
//
// Note: Fetches events for ALL tenants and principals.
// Multi-tenancy/principal filtering is handled at the aggregation rule level.
func (a *Adapter) RetrieveEventsAfter(ctx context.Context, afterTime time.Time, limit int) ([]*v1.Event, error) {
	rows, err := a.stmtRetrieveEvents.QueryContext(ctx, afterTime, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []*v1.Event
	for rows.Next() {
		event, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating events: %w", err)
	}

	return events, nil
}

// RetrieveEventsAfterCursor fetches events after a cursor (ingest_seq) in strict total order.
// Returns events ordered by ingest_seq ASC.
// Used by recovery to replay events without batch boundary data loss.
//
// Parameters:
//   - cursor: Last ingest_seq processed (fetch events with ingest_seq > cursor)
//   - limit: Maximum number of events to return
//
// Note: Fetches events for ALL tenants and principals.
// cursor=0 means "from the beginning"
func (a *Adapter) RetrieveEventsAfterCursor(ctx context.Context, cursor int64, limit int) ([]*v1.Event, error) {
	rows, err := a.stmtRetrieveEventsCursor.QueryContext(ctx, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query events by cursor: %w", err)
	}
	defer rows.Close()

	var events []*v1.Event
	for rows.Next() {
		event, err := scanEventRow(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating events: %w", err)
	}

	return events, nil
}

// RetrieveScopedEventsAfterCursor fetches events in strict order for one projection query scope.
func (a *Adapter) RetrieveScopedEventsAfterCursor(
	ctx context.Context,
	cursor int64,
	tenantID string,
	principalID string,
	eventType string,
	startOccurredAt time.Time,
	endOccurredAt time.Time,
	limit int,
) ([]*v1.Event, error) {
	rows, err := a.stmtRetrieveScopedCursor.QueryContext(
		ctx,
		cursor,
		tenantID,
		principalID,
		eventType,
		startOccurredAt,
		endOccurredAt,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query scoped events by cursor: %w", err)
	}
	defer rows.Close()

	var events []*v1.Event
	for rows.Next() {
		event, scanErr := scanEventRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating scoped events: %w", err)
	}

	return events, nil
}

// DB returns the underlying *sql.DB. Other postgres adapters (e.g. PreAggregateAdapter)
// share this connection rather than opening a second one.
func (a *Adapter) DB() *sql.DB {
	return a.db
}

// Close closes the database connection and all prepared statements.
// Should be called during graceful shutdown.
func (a *Adapter) Close() error {
	var firstErr error

	if err := a.stmtSaveEvent.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to close saveEvent statement: %w", err)
	}

	if err := a.stmtRetrieveEvents.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to close retrieveEvents statement: %w", err)
	}

	if err := a.stmtRetrieveEventsCursor.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to close retrieveEventsCursor statement: %w", err)
	}

	if err := a.stmtRetrieveScopedCursor.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to close retrieveScopedCursor statement: %w", err)
	}

	if err := a.db.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to close database: %w", err)
	}

	if firstErr != nil {
		return firstErr
	}

	slog.Info("[Postgres] Adapter closed gracefully")
	return nil
}
