package storage

import (
	"context"
	"errors"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
)

// ErrDuplicate is returned when an event with the same (tenant_id, principal_id, id) already exists.
var ErrDuplicate = errors.New("event already exists")

// EventStore defines the interface for storing and retrieving events.
type EventStore interface {
	SaveEvent(ctx context.Context, event *v1.Event) error

	// RetrieveEventsAfter - DEPRECATED: Use RetrieveEventsAfterCursor for recovery
	// Kept for backwards compatibility during migration.
	RetrieveEventsAfter(ctx context.Context, afterTime time.Time, limit int) ([]*v1.Event, error)

	// RetrieveEventsAfterCursor fetches events after a cursor (ingest_seq) in strict total order.
	// This prevents batch boundary data loss during recovery pagination.
	// cursor=0 means "from the beginning"
	RetrieveEventsAfterCursor(ctx context.Context, cursor int64, limit int) ([]*v1.Event, error)

	// RetrieveScopedEventsAfterCursor fetches events in strict total order for one query scope.
	// Used by the projection hybrid read path to merge unflushed raw events with pre-aggregates.
	RetrieveScopedEventsAfterCursor(
		ctx context.Context,
		cursor int64,
		tenantID string,
		principalID string,
		eventType string,
		startOccurredAt time.Time,
		endOccurredAt time.Time,
		limit int,
	) ([]*v1.Event, error)
}
