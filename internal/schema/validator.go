package schema

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"
)

// Validator validates event data against registered schemas.
type Validator struct {
	formatRegistry *FormatRegistry

	// Cache of compiled schemas
	mu           sync.RWMutex
	compiled     map[string]*CompiledSchema
	compileGroup singleflight.Group // Dedupe concurrent compilation
}

// NewValidator creates a new schema validator with format registry support.
func NewValidator(formatRegistry *FormatRegistry) *Validator {
	return &Validator{
		formatRegistry: formatRegistry,
		compiled:       make(map[string]*CompiledSchema),
	}
}

// RegisterFormat registers a format compiler and validator.
func (v *Validator) RegisterFormat(format Format, compiler FormatCompiler, validator FormatValidator) {
	v.formatRegistry.RegisterFormat(format, compiler, validator)
}

// validatorCacheKey generates a unique key for compiled schema cache.
// CRITICAL FIX: Include fingerprint to prevent cache collision when format changes.
func validatorCacheKey(schema *Schema) string {
	return fmt.Sprintf("%s:%s:%d:%s", schema.TenantID, schema.Type, schema.Version, schema.Fingerprint)
}

// ValidateData validates the event data against the schema.
func (v *Validator) ValidateData(ctx context.Context, schema *Schema, data map[string]interface{}) error {
	compiled, err := v.getOrCompile(ctx, schema)
	if err != nil {
		return err
	}

	// Get format-specific validator
	validator, err := v.formatRegistry.GetValidator(schema.Format)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	return validator.ValidateData(ctx, compiled, data)
}

// getOrCompile retrieves or compiles a schema.
// Uses singleflight to dedupe concurrent compilation of the same schema.
func (v *Validator) getOrCompile(ctx context.Context, schema *Schema) (*CompiledSchema, error) {
	key := validatorCacheKey(schema)

	// Check cache first
	v.mu.RLock()
	if compiled, exists := v.compiled[key]; exists {
		v.mu.RUnlock()
		return compiled, nil
	}
	v.mu.RUnlock()

	// Use singleflight to dedupe concurrent compilation
	result, err, _ := v.compileGroup.Do(key, func() (interface{}, error) {
		// Double-check cache after acquiring singleflight lock
		v.mu.RLock()
		if compiled, exists := v.compiled[key]; exists {
			v.mu.RUnlock()
			return compiled, nil
		}
		v.mu.RUnlock()

		// Get format-specific compiler
		compiler, err := v.formatRegistry.GetCompiler(schema.Format)
		if err != nil {
			return nil, fmt.Errorf("compilation failed: %w", err)
		}

		// Compile the schema
		compiled, err := compiler.Compile(ctx, schema)
		if err != nil {
			return nil, err
		}

		// Cache the result
		v.mu.Lock()
		v.compiled[key] = compiled
		v.mu.Unlock()

		return compiled, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*CompiledSchema), nil
}

// InvalidateCache removes a compiled schema from cache.
func (v *Validator) InvalidateCache(schema *Schema) {
	key := validatorCacheKey(schema)
	v.mu.Lock()
	delete(v.compiled, key)
	v.mu.Unlock()
}
