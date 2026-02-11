package yaml

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// SchemaSpec represents a compiled YAML schema specification.
// This is the runtime representation used for validation.
type SchemaSpec struct {
	Event       string            `yaml:"event"`
	Version     int               `yaml:"version"`
	Description string            `yaml:"description,omitempty"`
	StrictMode  bool              `yaml:"strictMode,omitempty"`
	Fields      map[string]*Field `yaml:"fields"`
}

// Field defines a single field in a YAML schema.
//
// Fields support two declaration styles:
//
//	Shorthand (scalar): user_id: string!
//	Long form (mapping): endpoint:
//	                        type: string!
//	                        minLength: 1
//
// Type names: string, bool, int32, int64, float, double
// Append "!" to mark a field as required.
type Field struct {
	// Type is the internal type tag: "string", "boolean", or "number".
	// Populated by UnmarshalYAML from the user-facing type name.
	Type string `yaml:"type"`

	// Kind specifies numeric precision: int32, int64, float, double.
	// Internal only — derived from the type name (e.g. "int32" → Type="number", Kind="int32").
	Kind string `yaml:"-"`

	// Required indicates if the field must be present (default: false).
	// Set by the "!" suffix on the type name, or explicitly via "required: true" in long form.
	Required bool `yaml:"required,omitempty"`

	// Nullable allows null values (computed from required flag for proto3 compatibility).
	// If required=true, null is rejected. If required=false, null is accepted.
	Nullable bool `yaml:"-"`

	// Enum restricts values to a specific set (for strings and numbers).
	Enum []interface{} `yaml:"enum,omitempty"`

	// Min/Max constraints for numbers.
	Min *float64 `yaml:"min,omitempty"`
	Max *float64 `yaml:"max,omitempty"`

	// String constraints.
	MinLength *int   `yaml:"minLength,omitempty"`
	MaxLength *int   `yaml:"maxLength,omitempty"`
	Pattern   string `yaml:"pattern,omitempty"`

	// Compiled regex (not serialized, populated during Validate).
	compiledPattern *regexp.Regexp `yaml:"-"`
}

// UnmarshalYAML implements custom unmarshaling to support both shorthand
// and long-form field declarations.
//
//	shorthand:  field_name: int32!
//	long form:  field_name:
//	              type: int32!
//	              min: 0
func (f *Field) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		return f.parseTypeString(value.Value)
	}

	// Long form: decode struct fields via alias (avoids infinite recursion),
	// then normalize the type string.
	type fieldAlias Field
	var alias fieldAlias
	if err := value.Decode(&alias); err != nil {
		return err
	}
	*f = Field(alias)

	if f.Type == "" {
		return fmt.Errorf("field missing 'type'")
	}
	return f.parseTypeString(f.Type)
}

// parseTypeString parses a user-facing type name like "int32!" and sets
// Type, Kind, and (if "!" is present) Required on the receiver.
//
// Recognized types: string, bool, int32, int64, float, double
func (f *Field) parseTypeString(s string) error {
	if strings.HasSuffix(s, "!") {
		f.Required = true
		s = strings.TrimSuffix(s, "!")
	}

	switch s {
	case "string":
		f.Type = "string"
	case "bool":
		f.Type = "boolean"
	case "int32", "int64", "float", "double":
		f.Type = "number"
		f.Kind = s
	default:
		return fmt.Errorf("unsupported type %q (must be: string, bool, int32, int64, float, double)", s)
	}
	return nil
}

// Validate checks if the YAML schema spec is structurally valid.
// This is called during schema compilation to catch definition errors.
func (s *SchemaSpec) Validate() error {
	if s.Event == "" {
		return fmt.Errorf("event type is required")
	}
	if s.Version < 1 {
		return fmt.Errorf("version must be >= 1")
	}
	if len(s.Fields) == 0 {
		return fmt.Errorf("schema must define at least one field")
	}

	for name, field := range s.Fields {
		if field == nil {
			return fmt.Errorf("field %q: type cannot be empty", name)
		}
		if err := field.Validate(name); err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
	}

	return nil
}

// Validate checks if a field definition is structurally valid.
func (f *Field) Validate(path string) error {
	switch f.Type {
	case "string":
		return f.validateStringField(path)
	case "boolean":
		return f.validateBooleanField(path)
	case "number":
		return f.validateNumberField(path)
	default:
		return fmt.Errorf("unsupported type %q (must be: string, bool, int32, int64, float, double)", f.Type)
	}
}

// validateStringField validates string-specific constraints.
func (f *Field) validateStringField(path string) error {
	if f.MinLength != nil && *f.MinLength < 0 {
		return fmt.Errorf("minLength cannot be negative")
	}
	if f.MaxLength != nil && *f.MaxLength < 0 {
		return fmt.Errorf("maxLength cannot be negative")
	}
	if f.MinLength != nil && f.MaxLength != nil && *f.MinLength > *f.MaxLength {
		return fmt.Errorf("minLength (%d) cannot exceed maxLength (%d)", *f.MinLength, *f.MaxLength)
	}

	if f.Pattern != "" {
		if len(f.Pattern) > 1000 {
			return fmt.Errorf("pattern too long (max 1000 chars)")
		}
		compiled, err := regexp.Compile(f.Pattern)
		if err != nil {
			return fmt.Errorf("invalid regex pattern: %w", err)
		}
		f.compiledPattern = compiled
	}

	if len(f.Enum) > 0 {
		for i, val := range f.Enum {
			if _, ok := val.(string); !ok {
				return fmt.Errorf("enum[%d]: expected string, got %T", i, val)
			}
		}
	}

	return nil
}

// validateBooleanField validates boolean-specific constraints.
func (f *Field) validateBooleanField(path string) error {
	if f.MinLength != nil || f.MaxLength != nil || f.Pattern != "" {
		return fmt.Errorf("boolean fields do not support length or pattern constraints")
	}
	if f.Min != nil || f.Max != nil {
		return fmt.Errorf("boolean fields do not support min/max constraints")
	}
	if len(f.Enum) > 0 {
		return fmt.Errorf("boolean fields do not support enum constraints")
	}
	return nil
}

// validateNumberField validates number-specific constraints.
func (f *Field) validateNumberField(path string) error {
	if f.Kind == "" {
		return fmt.Errorf("number type requires kind (int32, int64, float, or double)")
	}

	switch f.Kind {
	case "int32", "int64", "float", "double":
		// valid
	default:
		return fmt.Errorf("invalid number kind %q (must be: int32, int64, float, double)", f.Kind)
	}

	if f.Min != nil && f.Max != nil && *f.Min > *f.Max {
		return fmt.Errorf("min (%v) cannot exceed max (%v)", *f.Min, *f.Max)
	}

	if len(f.Enum) > 0 {
		for i, val := range f.Enum {
			switch val.(type) {
			case int, int32, int64, float32, float64:
				// ok
			default:
				return fmt.Errorf("enum[%d]: expected number, got %T", i, val)
			}
		}
	}

	if f.MinLength != nil || f.MaxLength != nil || f.Pattern != "" {
		return fmt.Errorf("number fields do not support length or pattern constraints")
	}

	return nil
}

// IsRequired returns true if the field must be present.
func (f *Field) IsRequired() bool {
	return f.Required
}

// IsNullable returns true if null values are accepted.
// Follows proto3 semantics: required=false means nullable=true
func (f *Field) IsNullable() bool {
	return !f.Required
}

// HasConstraints returns true if the field has validation constraints beyond type.
func (f *Field) HasConstraints() bool {
	return len(f.Enum) > 0 ||
		f.Min != nil ||
		f.Max != nil ||
		f.MinLength != nil ||
		f.MaxLength != nil ||
		f.Pattern != ""
}

// String returns a human-readable description of the field type.
func (f *Field) String() string {
	var parts []string
	parts = append(parts, f.Type)

	if f.Kind != "" {
		parts = append(parts, fmt.Sprintf("(%s)", f.Kind))
	}

	if f.Required {
		parts = append(parts, "required")
	}

	if len(f.Enum) > 0 {
		parts = append(parts, fmt.Sprintf("enum[%d]", len(f.Enum)))
	}

	return strings.Join(parts, " ")
}
