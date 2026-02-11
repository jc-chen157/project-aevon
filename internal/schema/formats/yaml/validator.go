package yaml

import (
	"context"
	"fmt"

	"github.com/aevon-lab/project-aevon/internal/schema"
)

// Validator validates event data against YAML schemas.
type Validator struct{}

// NewValidator creates a new YAML validator.
func NewValidator() *Validator {
	return &Validator{}
}

// ValidateData validates the event data against the compiled YAML schema.
func (v *Validator) ValidateData(ctx context.Context, compiled *schema.CompiledSchema, data map[string]interface{}) error {
	// Get the YAML spec
	specIntf, err := compiled.GetYAMLSpec()
	if err != nil {
		return err
	}
	spec, ok := specIntf.(*SchemaSpec)
	if !ok {
		return fmt.Errorf("compile schema is not a YAML SchemaSpec: %T", specIntf)
	}

	// Check for unknown fields in strict mode
	if compiled.StrictMode {
		var unknownFields []string
		for key := range data {
			if _, exists := spec.Fields[key]; !exists {
				unknownFields = append(unknownFields, key)
			}
		}
		if len(unknownFields) > 0 {
			return schema.NewUnknownFieldsError(compiled.EventType, compiled.Version, unknownFields)
		}
	}

	// Validate each field in the schema
	var errors []*schema.ValidationError
	for fieldName, fieldSpec := range spec.Fields {
		value, exists := data[fieldName]

		// Check required fields
		if fieldSpec.Required && !exists {
			errors = append(errors, &schema.ValidationError{
				Schema:  compiled.EventType,
				Version: compiled.Version,
				Field:   fieldName,
				Message: "required field is missing",
				Format:  string(schema.FormatYaml),
			})
			continue
		}

		// Skip validation if field not present (and not required)
		if !exists {
			continue
		}

		// Validate the field value
		if err := v.validateField(compiled, fieldName, fieldSpec, value); err != nil {
			if ve, ok := err.(*schema.ValidationError); ok {
				ve.Format = string(schema.FormatYaml) // Include format in error
				errors = append(errors, ve)
			} else {
				errors = append(errors, &schema.ValidationError{
					Schema:  compiled.EventType,
					Version: compiled.Version,
					Field:   fieldName,
					Message: err.Error(),
					Format:  string(schema.FormatYaml),
				})
			}
		}
	}

	if len(errors) > 0 {
		return &schema.MultiValidationError{Errors: errors}
	}

	return nil
}

// validateField validates a single field value against its spec.
func (v *Validator) validateField(s *schema.CompiledSchema, fieldName string, spec *Field, value interface{}) error {
	// Handle null values
	if value == nil {
		if spec.Required {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: "required field cannot be null",
			}
		}
		return nil // Nullable field
	}

	// Dispatch by type
	switch spec.Type {
	case "string":
		return v.validateString(s, fieldName, spec, value)
	case "boolean":
		return v.validateBoolean(s, fieldName, spec, value)
	case "number":
		return v.validateNumber(s, fieldName, spec, value)
	default:
		return &schema.ValidationError{
			Schema:  s.EventType,
			Version: s.Version,
			Field:   fieldName,
			Message: fmt.Sprintf("unknown field type: %s", spec.Type),
		}
	}
}

// validateString validates a string field.
func (v *Validator) validateString(s *schema.CompiledSchema, fieldName string, spec *Field, value interface{}) error {
	str, ok := value.(string)
	if !ok {
		return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "string", jsonTypeName(value))
	}

	// Enum constraint
	if len(spec.Enum) > 0 {
		found := false
		for _, allowed := range spec.Enum {
			if allowedStr, ok := allowed.(string); ok && allowedStr == str {
				found = true
				break
			}
		}
		if !found {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: fmt.Sprintf("value %q not in enum %v", str, spec.Enum),
			}
		}
	}

	// Length constraints
	length := len(str)
	if spec.MinLength != nil && length < *spec.MinLength {
		return &schema.ValidationError{
			Schema:  s.EventType,
			Version: s.Version,
			Field:   fieldName,
			Message: fmt.Sprintf("string length %d is less than minimum %d", length, *spec.MinLength),
		}
	}
	if spec.MaxLength != nil && length > *spec.MaxLength {
		return &schema.ValidationError{
			Schema:  s.EventType,
			Version: s.Version,
			Field:   fieldName,
			Message: fmt.Sprintf("string length %d exceeds maximum %d", length, *spec.MaxLength),
		}
	}

	// Pattern constraint (regex)
	if spec.compiledPattern != nil {
		if !spec.compiledPattern.MatchString(str) {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: fmt.Sprintf("string does not match pattern %q", spec.Pattern),
			}
		}
	}

	return nil
}

// validateBoolean validates a boolean field.
func (v *Validator) validateBoolean(s *schema.CompiledSchema, fieldName string, spec *Field, value interface{}) error {
	if _, ok := value.(bool); !ok {
		return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "boolean", jsonTypeName(value))
	}
	return nil
}

// validateNumber validates a number field with strict overflow protection.
func (v *Validator) validateNumber(s *schema.CompiledSchema, fieldName string, spec *Field, value interface{}) error {
	// JSON unmarshals all numbers as float64
	num, ok := value.(float64)
	if !ok {
		// Also accept integers from YAML parser
		if intVal, ok := value.(int); ok {
			num = float64(intVal)
		} else {
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "number", jsonTypeName(value))
		}
	}

	// CRITICAL FIX: Validate number kind and range to prevent overflow
	switch spec.Kind {
	case "int32":
		// Reject fractional values
		if num != float64(int64(num)) {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: "expected integer, got float with fractional part",
			}
		}
		// Check int32 range: -2,147,483,648 to 2,147,483,647
		if num < -2147483648 || num > 2147483647 {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: fmt.Sprintf("value %v out of range for int32 (min: -2147483648, max: 2147483647)", num),
			}
		}

	case "int64":
		// Reject fractional values
		if num != float64(int64(num)) {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: "expected integer, got float with fractional part",
			}
		}
		// Check int64 range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
		// Note: float64 can't represent all int64 values precisely, but this catches obvious overflows
		if num < -9223372036854775808 || num > 9223372036854775807 {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: fmt.Sprintf("value %v out of range for int64", num),
			}
		}

	case "float":
		// float32 range: approximately ±3.4e38
		// We allow float64 values but warn if they exceed float32 range
		if num != 0 && (num < -3.4e38 || num > 3.4e38) {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: fmt.Sprintf("value %v out of range for float32", num),
			}
		}

	case "double":
		// float64 - no additional validation needed
		// Go's float64 matches protobuf's double

	default:
		return &schema.ValidationError{
			Schema:  s.EventType,
			Version: s.Version,
			Field:   fieldName,
			Message: fmt.Sprintf("unknown number kind: %s", spec.Kind),
		}
	}

	// Enum constraint
	if len(spec.Enum) > 0 {
		found := false
		for _, allowed := range spec.Enum {
			var allowedNum float64
			switch v := allowed.(type) {
			case int:
				allowedNum = float64(v)
			case int32:
				allowedNum = float64(v)
			case int64:
				allowedNum = float64(v)
			case float32:
				allowedNum = float64(v)
			case float64:
				allowedNum = v
			default:
				continue
			}
			if allowedNum == num {
				found = true
				break
			}
		}
		if !found {
			return &schema.ValidationError{
				Schema:  s.EventType,
				Version: s.Version,
				Field:   fieldName,
				Message: fmt.Sprintf("value %v not in enum %v", num, spec.Enum),
			}
		}
	}

	// Min/max constraints
	if spec.Min != nil && num < *spec.Min {
		return &schema.ValidationError{
			Schema:  s.EventType,
			Version: s.Version,
			Field:   fieldName,
			Message: fmt.Sprintf("value %v is less than minimum %v", num, *spec.Min),
		}
	}
	if spec.Max != nil && num > *spec.Max {
		return &schema.ValidationError{
			Schema:  s.EventType,
			Version: s.Version,
			Field:   fieldName,
			Message: fmt.Sprintf("value %v exceeds maximum %v", num, *spec.Max),
		}
	}

	return nil
}

// jsonTypeName returns a human-readable type name for JSON values.
func jsonTypeName(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}
