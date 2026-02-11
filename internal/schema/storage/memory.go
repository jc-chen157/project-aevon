package storage

import (
	"context"
	"sync"

	"github.com/aevon-lab/project-aevon/internal/schema"
)

// MemoryRepository is an in-memory implementation of Repository.
// Useful for testing and development.
type MemoryRepository struct {
	mu      sync.RWMutex
	schemas map[schema.Key]*schema.Schema
}

// NewMemoryRepository creates a new in-memory schema repository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		schemas: make(map[schema.Key]*schema.Schema),
	}
}

func (r *MemoryRepository) Create(ctx context.Context, s *schema.Schema) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := s.Key()
	if _, exists := r.schemas[key]; exists {
		return schema.ErrAlreadyExists
	}

	// Store a copy to prevent external modification
	copy := *s
	r.schemas[key] = &copy
	return nil
}

func (r *MemoryRepository) Get(ctx context.Context, key schema.Key) (*schema.Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, exists := r.schemas[key]
	if !exists {
		return nil, schema.ErrNotFound
	}

	// Return a copy to prevent external modification
	copy := *s
	return &copy, nil
}

func (r *MemoryRepository) List(ctx context.Context, tenantID string, eventType string) ([]*schema.Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*schema.Schema
	for _, s := range r.schemas {
		if s.TenantID != tenantID {
			continue
		}
		if eventType != "" && s.Type != eventType {
			continue
		}
		copy := *s
		result = append(result, &copy)
	}
	return result, nil
}

func (r *MemoryRepository) UpdateState(ctx context.Context, key schema.Key, state schema.State) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	s, exists := r.schemas[key]
	if !exists {
		return schema.ErrNotFound
	}

	s.State = state
	return nil
}

func (r *MemoryRepository) Delete(ctx context.Context, key schema.Key) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.schemas[key]; !exists {
		return schema.ErrNotFound
	}

	delete(r.schemas, key)
	return nil
}
