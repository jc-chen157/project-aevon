package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// PlatformTenantID is the reserved tenant ID for platform-provided schemas.
const PlatformTenantID = "_platform"

// State represents the lifecycle state of a schema.
type State string

const (
	StateActive     State = "active"
	StateDeprecated State = "deprecated"
	StateDeleted    State = "deleted"
)

// Format represents the format of the schema definition.
type Format string

const (
	FormatProtobuf Format = "protobuf"
	FormatYaml     Format = "yaml"
	FormatJSON     Format = "json"
)

// Schema represents a registered event schema.
type Schema struct {
	// ID is the unique schema identifier (UUID).
	ID string `json:"id"`

	// TenantID isolates schemas per tenant. "_platform" for global schemas.
	TenantID string `json:"tenant_id"`

	// Type is the event type this schema validates (e.g., "api.request").
	Type string `json:"type"`

	// Version is the schema version number (1, 2, 3...).
	Version int `json:"version"`

	// Format is the schema format (protobuf for v1).
	Format Format `json:"format"`

	// Definition is the raw schema content (e.g., .proto file content).
	Definition []byte `json:"definition"`

	// Compiled is the cached compiled FileDescriptorProto.
	Compiled []byte `json:"compiled,omitempty"`

	// Fingerprint is SHA-256 hash of Definition for deduplication.
	Fingerprint string `json:"fingerprint"`

	// State is the lifecycle state of the schema.
	State State `json:"state"`

	// StrictMode rejects events with unknown fields when true.
	StrictMode bool `json:"strict_mode"`

	// CreatedAt is when the schema was registered.
	CreatedAt time.Time `json:"created_at"`

	// DeprecatedAt is when the schema was deprecated (nil if active).
	DeprecatedAt *time.Time `json:"deprecated_at,omitempty"`
}

// ComputeFingerprint calculates SHA-256 hash of the definition.
func ComputeFingerprint(definition []byte) string {
	hash := sha256.Sum256(definition)
	return hex.EncodeToString(hash[:])
}

// Key uniquely identifies a schema for lookup.
type Key struct {
	TenantID string
	Type     string
	Version  int
}

// Key returns the lookup key for this schema.
func (s *Schema) Key() Key {
	return Key{
		TenantID: s.TenantID,
		Type:     s.Type,
		Version:  s.Version,
	}
}

// CompiledSchema represents a compiled schema ready for validation.
// Uses discriminated union pattern for type safety across formats.
type CompiledSchema struct {
	EventType  string
	Version    int
	Format     Format // Required discriminator - identifies which field is populated
	StrictMode bool

	// Discriminated union - exactly one is populated based on Format
	ProtoDescriptor *protoreflect.MessageDescriptor // When Format = FormatProtobuf
	YAMLSpec        interface{}                     // When Format = FormatYaml (yaml.SchemaSpec)
}

// GetProtoDescriptor returns the protobuf descriptor if this is a protobuf schema.
// Returns error if the schema is not in protobuf format.
func (c *CompiledSchema) GetProtoDescriptor() (protoreflect.MessageDescriptor, error) {
	if c.Format != FormatProtobuf || c.ProtoDescriptor == nil {
		return nil, fmt.Errorf("not a protobuf schema (format: %s)", c.Format)
	}
	return *c.ProtoDescriptor, nil
}

// GetYAMLSpec returns the YAML spec if this is a YAML schema.
// Returns error if the schema is not in YAML format.
// The returned value should be type-asserted to *yaml.SchemaSpec.
func (c *CompiledSchema) GetYAMLSpec() (interface{}, error) {
	if c.Format != FormatYaml || c.YAMLSpec == nil {
		return nil, fmt.Errorf("not a YAML schema (format: %s)", c.Format)
	}
	return c.YAMLSpec, nil
}
