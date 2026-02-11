package schema

import (
	"errors"
	"fmt"
	"strings"
)

// Common errors
var (
	// ErrNotFound is returned when a schema is not found in the repository.
	ErrNotFound      = errors.New("schema not found")
	ErrAlreadyExists = errors.New("schema already exists")
	ErrDeprecated    = errors.New("schema is deprecated")
)

// ValidationError represents a schema validation failure.
type ValidationError struct {
	Schema        string   `json:"schema"`
	Version       int      `json:"version"`
	Format        string   `json:"format,omitempty"` // Schema format for debugging
	Message       string   `json:"message"`
	Field         string   `json:"field,omitempty"`
	ExpectedType  string   `json:"expected_type,omitempty"`
	ActualType    string   `json:"actual_type,omitempty"`
	UnknownFields []string `json:"unknown_fields,omitempty"`
}

func (e *ValidationError) Error() string {
	if len(e.UnknownFields) > 0 {
		return fmt.Sprintf("unknown field(s) %v not allowed in schema %s v%d",
			e.UnknownFields, e.Schema, e.Version)
	}
	if e.Field != "" {
		return fmt.Sprintf("field '%s': %s (schema %s v%d)",
			e.Field, e.Message, e.Schema, e.Version)
	}
	return fmt.Sprintf("%s (schema %s v%d)", e.Message, e.Schema, e.Version)
}

// MultiValidationError aggregates multiple validation errors.
type MultiValidationError struct {
	Errors []*ValidationError
}

func (e *MultiValidationError) Error() string {
	if len(e.Errors) == 0 {
		return "validation failed"
	}
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}

	msgs := make([]string, len(e.Errors))
	for i, err := range e.Errors {
		msgs[i] = err.Error()
	}
	return fmt.Sprintf("validation failed: %s", strings.Join(msgs, "; "))
}

// ValidationDetailer surfaces structured validation details for API error responses.
// Implemented by all validation error types so consumers extract details without
// type-asserting against concrete structs.
type ValidationDetailer interface {
	Details() map[string]interface{}
}

// Details returns the structured fields from this single validation error.
func (e *ValidationError) Details() map[string]interface{} {
	d := make(map[string]interface{})
	if len(e.UnknownFields) > 0 {
		d["unknown_fields"] = e.UnknownFields
	}
	if e.Field != "" {
		d["field"] = e.Field
	}
	return d
}

// Details aggregates the failed field names from all child errors.
func (e *MultiValidationError) Details() map[string]interface{} {
	d := make(map[string]interface{})
	var fields []string
	for _, ve := range e.Errors {
		if ve.Field != "" {
			fields = append(fields, ve.Field)
		}
	}
	if len(fields) > 0 {
		d["fields"] = fields
	}
	return d
}

// NewUnknownFieldsError creates an error for unexpected fields.
func NewUnknownFieldsError(schema string, version int, fields []string) *ValidationError {
	return &ValidationError{
		Schema:        schema,
		Version:       version,
		Message:       fmt.Sprintf("unknown field(s) not allowed: %v", fields),
		UnknownFields: fields,
	}
}

// NewTypeMismatchError creates an error for type mismatches.
func NewTypeMismatchError(schema string, version int, field, expected, actual string) *ValidationError {
	return &ValidationError{
		Schema:       schema,
		Version:      version,
		Message:      fmt.Sprintf("expected %s, got %s", expected, actual),
		Field:        field,
		ExpectedType: expected,
		ActualType:   actual,
	}
}

// NewRequiredFieldError creates an error for missing required fields.
func NewRequiredFieldError(schema string, version int, field string) *ValidationError {
	return &ValidationError{
		Schema:  schema,
		Version: version,
		Message: "required field is missing",
		Field:   field,
	}
}
