package schema

import (
	"context"
	"fmt"
	"sync"
)

// FormatCompiler compiles schema definitions into runtime representations.
// Each schema format (protobuf, YAML, JSON) implements this interface.
type FormatCompiler interface {
	// Compile parses the schema definition and returns a compiled schema.
	// Returns error if the definition is malformed or invalid.
	Compile(ctx context.Context, schema *Schema) (*CompiledSchema, error)
}

// FormatValidator validates event data against compiled schemas.
// Each schema format provides format-specific validation logic.
type FormatValidator interface {
	// ValidateData checks if the event data conforms to the compiled schema.
	// Returns validation errors if data is invalid.
	ValidateData(ctx context.Context, compiled *CompiledSchema, data map[string]interface{}) error
}

// FormatRegistry manages compiler and validator implementations for each schema format.
// It acts as a central registry for pluggable format support.
type FormatRegistry struct {
	mu         sync.RWMutex
	compilers  map[Format]FormatCompiler
	validators map[Format]FormatValidator
}

// NewFormatRegistry creates a new format registry.
func NewFormatRegistry() *FormatRegistry {
	return &FormatRegistry{
		compilers:  make(map[Format]FormatCompiler),
		validators: make(map[Format]FormatValidator),
	}
}

// RegisterFormat registers compiler and validator for a schema format.
// This should be called during initialization to enable format support.
func (r *FormatRegistry) RegisterFormat(format Format, compiler FormatCompiler, validator FormatValidator) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.compilers[format] = compiler
	r.validators[format] = validator
}

// GetCompiler retrieves the compiler for a given format.
// Returns error if the format is not registered.
func (r *FormatRegistry) GetCompiler(format Format) (FormatCompiler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	compiler, exists := r.compilers[format]
	if !exists {
		return nil, fmt.Errorf("unsupported schema format: %s", format)
	}
	return compiler, nil
}

// GetValidator retrieves the validator for a given format.
// Returns error if the format is not registered.
func (r *FormatRegistry) GetValidator(format Format) (FormatValidator, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	validator, exists := r.validators[format]
	if !exists {
		return nil, fmt.Errorf("unsupported schema format: %s", format)
	}
	return validator, nil
}

// IsFormatSupported checks if a format has been registered.
func (r *FormatRegistry) IsFormatSupported(format Format) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.compilers[format]
	return exists
}

// SupportedFormats returns a list of all registered formats.
func (r *FormatRegistry) SupportedFormats() []Format {
	r.mu.RLock()
	defer r.mu.RUnlock()

	formats := make([]Format, 0, len(r.compilers))
	for format := range r.compilers {
		formats = append(formats, format)
	}
	return formats
}
