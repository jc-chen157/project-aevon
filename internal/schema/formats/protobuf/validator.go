package protobuf

import (
	"context"
	"fmt"

	"github.com/aevon-lab/project-aevon/internal/schema"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Validator validates event data against protobuf schemas.
type Validator struct{}

// NewValidator creates a new protobuf validator.
func NewValidator() *Validator {
	return &Validator{}
}

// ValidateData validates the event data against the compiled protobuf schema.
func (v *Validator) ValidateData(ctx context.Context, compiled *schema.CompiledSchema, data map[string]interface{}) error {
	// Get the protobuf descriptor
	msgDesc, err := compiled.GetProtoDescriptor()
	if err != nil {
		return err
	}

	return v.validateMessage(compiled, msgDesc, data)
}

// validateMessage validates a JSON object against a message descriptor.
func (v *Validator) validateMessage(s *schema.CompiledSchema, md protoreflect.MessageDescriptor, data map[string]interface{}) error {
	fields := md.Fields()

	// Build a set of known field names (both JSON name and proto name)
	knownFields := make(map[string]protoreflect.FieldDescriptor)
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		knownFields[fd.JSONName()] = fd
		knownFields[string(fd.Name())] = fd
	}

	// In strict mode, check for unknown fields
	if s.StrictMode {
		var unknownFields []string
		for key := range data {
			if _, ok := knownFields[key]; !ok {
				unknownFields = append(unknownFields, key)
			}
		}
		if len(unknownFields) > 0 {
			return schema.NewUnknownFieldsError(s.EventType, s.Version, unknownFields)
		}
	}

	// Validate each field in the data
	var errors []*schema.ValidationError
	for key, value := range data {
		fd, ok := knownFields[key]
		if !ok {
			// Unknown field in non-strict mode - skip
			continue
		}

		if err := v.validateField(s, fd, value); err != nil {
			if ve, ok := err.(*schema.ValidationError); ok {
				errors = append(errors, ve)
			} else {
				errors = append(errors, &schema.ValidationError{
					Schema:  s.EventType,
					Version: s.Version,
					Field:   key,
					Message: err.Error(),
				})
			}
		}
	}

	if len(errors) > 0 {
		return &schema.MultiValidationError{Errors: errors}
	}

	return nil
}

// validateField validates a single field value against its descriptor.
func (v *Validator) validateField(s *schema.CompiledSchema, fd protoreflect.FieldDescriptor, value interface{}) error {
	fieldName := fd.JSONName()

	// Handle null values
	if value == nil {
		return nil // proto3 fields are optional by default
	}

	// Handle repeated fields
	if fd.IsList() {
		arr, ok := value.([]interface{})
		if !ok {
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "array", jsonTypeName(value))
		}
		for i, elem := range arr {
			if err := v.validateScalarField(s, fd, fmt.Sprintf("%s[%d]", fieldName, i), elem); err != nil {
				return err
			}
		}
		return nil
	}

	// Handle map fields
	if fd.IsMap() {
		m, ok := value.(map[string]interface{})
		if !ok {
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "object", jsonTypeName(value))
		}
		valueDesc := fd.MapValue()
		for k, mapValue := range m {
			if err := v.validateScalarField(s, valueDesc, fmt.Sprintf("%s[%q]", fieldName, k), mapValue); err != nil {
				return err
			}
		}
		return nil
	}

	return v.validateScalarField(s, fd, fieldName, value)
}

// validateScalarField validates a scalar field value.
func (v *Validator) validateScalarField(s *schema.CompiledSchema, fd protoreflect.FieldDescriptor, fieldName string, value interface{}) error {
	if value == nil {
		return nil
	}

	kind := fd.Kind()

	switch kind {
	case protoreflect.BoolKind:
		if _, ok := value.(bool); !ok {
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "bool", jsonTypeName(value))
		}

	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		// JSON numbers can be float64 in Go
		switch value.(type) {
		case float64, int, int32, int64:
			// ok
		default:
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "integer", jsonTypeName(value))
		}

	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		switch value.(type) {
		case float64, int, int32, int64, uint, uint32, uint64:
			// ok
		default:
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "unsigned integer", jsonTypeName(value))
		}

	case protoreflect.FloatKind, protoreflect.DoubleKind:
		switch value.(type) {
		case float64, float32, int, int32, int64:
			// ok
		default:
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "number", jsonTypeName(value))
		}

	case protoreflect.StringKind:
		if _, ok := value.(string); !ok {
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "string", jsonTypeName(value))
		}

	case protoreflect.BytesKind:
		// bytes in JSON is base64 encoded string
		if _, ok := value.(string); !ok {
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "string (base64)", jsonTypeName(value))
		}

	case protoreflect.EnumKind:
		// Enums can be string (name) or number (value)
		switch value.(type) {
		case string, float64, int:
			// ok
		default:
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "string or integer (enum)", jsonTypeName(value))
		}

	case protoreflect.MessageKind:
		// Nested message
		m, ok := value.(map[string]interface{})
		if !ok {
			return schema.NewTypeMismatchError(s.EventType, s.Version, fieldName, "object", jsonTypeName(value))
		}
		// Create a nested CompiledSchema for recursive validation
		msgDesc := fd.Message()
		nestedSchema := &schema.CompiledSchema{
			EventType:       s.EventType,
			Version:         s.Version,
			Format:          schema.FormatProtobuf,
			StrictMode:      s.StrictMode,
			ProtoDescriptor: &msgDesc,
		}
		return v.validateMessage(nestedSchema, msgDesc, m)

	case protoreflect.GroupKind:
		// Groups are deprecated in proto3
		return nil
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
