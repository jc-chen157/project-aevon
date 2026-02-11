package schema

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// DefaultCacheCapacity is the default number of schemas to cache.
const DefaultCacheCapacity = 1000

// Registry provides schema lookup with caching and hybrid tenant/platform fallback.
type Registry struct {
	repo  Repository
	cache *LRUCache
}

// NewRegistry creates a new schema registry.
func NewRegistry(repo Repository) *Registry {
	return &Registry{
		repo:  repo,
		cache: NewLRUCache(DefaultCacheCapacity),
	}
}

// NewRegistryWithCache creates a registry with a custom cache capacity.
func NewRegistryWithCache(repo Repository, cacheCapacity int) *Registry {
	return &Registry{
		repo:  repo,
		cache: NewLRUCache(cacheCapacity),
	}
}

// Get retrieves a schema using hybrid lookup:
// 1. Try tenant-specific schema first
// 2. Fallback to platform global schema
func (r *Registry) Get(ctx context.Context, tenantID, eventType string, version int) (*Schema, error) {
	// Try tenant-specific schema
	key := Key{TenantID: tenantID, Type: eventType, Version: version}
	schema, err := r.getWithCache(ctx, key)
	if err == nil {
		return schema, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	// Fallback to platform global schema
	platformKey := Key{TenantID: PlatformTenantID, Type: eventType, Version: version}
	schema, err = r.getWithCache(ctx, platformKey)
	if err == nil {
		return schema, nil
	}
	if errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("schema not found: %s v%d", eventType, version)
	}
	return nil, err
}

// getWithCache retrieves a schema from cache or repository.
func (r *Registry) getWithCache(ctx context.Context, key Key) (*Schema, error) {
	// Check cache first
	if schema := r.cache.Get(key); schema != nil {
		return schema, nil
	}

	// Cache miss - fetch from repository
	schema, err := r.repo.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// Populate cache
	r.cache.Put(schema)
	return schema, nil
}

// Register creates a new schema version.
func (r *Registry) Register(ctx context.Context, tenantID, eventType string, version int, format Format, definition []byte, strictMode bool) (*Schema, error) {
	if tenantID == "" {
		return nil, errors.New("tenant_id is required")
	}
	if eventType == "" {
		return nil, errors.New("type is required")
	}
	if version < 1 {
		return nil, errors.New("version must be >= 1")
	}
	if len(definition) == 0 {
		return nil, errors.New("definition is required")
	}

	schema := &Schema{
		ID:          uuid.New().String(),
		TenantID:    tenantID,
		Type:        eventType,
		Version:     version,
		Format:      format,
		Definition:  definition,
		Fingerprint: ComputeFingerprint(definition),
		State:       StateActive,
		StrictMode:  strictMode,
		CreatedAt:   time.Now().UTC(),
	}

	if err := r.repo.Create(ctx, schema); err != nil {
		return nil, err
	}

	// Populate cache
	r.cache.Put(schema)
	return schema, nil
}

// Deprecate marks a schema as deprecated.
func (r *Registry) Deprecate(ctx context.Context, tenantID, eventType string, version int) error {
	key := Key{TenantID: tenantID, Type: eventType, Version: version}

	if err := r.repo.UpdateState(ctx, key, StateDeprecated); err != nil {
		return err
	}

	// Invalidate cache
	r.cache.Invalidate(key)
	return nil
}

// List returns all schemas for a tenant.
func (r *Registry) List(ctx context.Context, tenantID, eventType string) ([]*Schema, error) {
	return r.repo.List(ctx, tenantID, eventType)
}
