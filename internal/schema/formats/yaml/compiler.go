package yaml

import (
	"context"
	"fmt"

	"github.com/aevon-lab/project-aevon/internal/schema"
	"gopkg.in/yaml.v3"
)

// Compiler compiles YAML schema definitions.
type Compiler struct{}

// NewCompiler creates a new YAML compiler.
func NewCompiler() *Compiler {
	return &Compiler{}
}

// Compile parses a YAML schema definition and returns the compiled schema.
func (c *Compiler) Compile(ctx context.Context, s *schema.Schema) (*schema.CompiledSchema, error) {
	// Validate format
	if s.Format != schema.FormatYaml {
		return nil, fmt.Errorf("expected yaml format, got %s", s.Format)
	}

	// Parse YAML
	var spec SchemaSpec
	if err := yaml.Unmarshal(s.Definition, &spec); err != nil {
		return nil, fmt.Errorf("failed to parse YAML schema: %w", err)
	}

	// Validate schema structure
	if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("invalid YAML schema: %w", err)
	}

	// Ensure event type matches
	if spec.Event != s.Type {
		return nil, fmt.Errorf("schema event type %q does not match schema.Type %q", spec.Event, s.Type)
	}

	// Ensure version matches
	if spec.Version != s.Version {
		return nil, fmt.Errorf("schema version %d does not match schema.Version %d", spec.Version, s.Version)
	}

	// Set nullable flag on all fields based on required flag (proto3 semantics)
	for _, field := range spec.Fields {
		field.Nullable = !field.Required
	}

	return &schema.CompiledSchema{
		EventType:  s.Type,
		Version:    s.Version,
		Format:     schema.FormatYaml,
		StrictMode: s.StrictMode || spec.StrictMode, // Honor both flags
		YAMLSpec:   &spec,
	}, nil
}
