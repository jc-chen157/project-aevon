package storage

import (
	"context"
	"errors"

	"github.com/aevon-lab/project-aevon/internal/schema"
)

// Common errors returned by the repository.
var (
	ErrAlreadyExists = errors.New("schema already exists")
	ErrDeprecated    = errors.New("schema is deprecated")
)

// Repository defines the interface for schema storage.
type Repository interface {
	// Create stores a new schema. Returns ErrAlreadyExists if
	// a schema with the same (TenantID, Type, Version) already exists.
	Create(ctx context.Context, schema *schema.Schema) error

	// Get retrieves a schema by key. Returns schema.ErrNotFound if not found.
	Get(ctx context.Context, key schema.Key) (*schema.Schema, error)

	// List returns all schemas for a tenant, optionally filtered by type.
	List(ctx context.Context, tenantID string, eventType string) ([]*schema.Schema, error)

	// UpdateState changes the state of a schema (e.g., deprecate).
	UpdateState(ctx context.Context, key schema.Key, state schema.State) error

	// Delete removes a schema. This is a hard delete.
	Delete(ctx context.Context, key schema.Key) error
}
