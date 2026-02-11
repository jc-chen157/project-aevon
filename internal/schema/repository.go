package schema

import (
	"context"
)

// Repository defines the interface for schema storage.
type Repository interface {
	// Create stores a new schema. Returns ErrAlreadyExists if
	// a schema with the same (TenantID, Type, Version) already exists.
	Create(ctx context.Context, schema *Schema) error

	// Get retrieves a schema by key. Returns ErrNotFound if not found.
	Get(ctx context.Context, key Key) (*Schema, error)

	// List returns all schemas for a tenant, optionally filtered by type.
	List(ctx context.Context, tenantID string, eventType string) ([]*Schema, error)

	// UpdateState changes the state of a schema (e.g., deprecate).
	UpdateState(ctx context.Context, key Key, state State) error

	// Delete removes a schema. This is a hard delete.
	Delete(ctx context.Context, key Key) error
}
