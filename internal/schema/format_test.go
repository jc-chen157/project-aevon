package schema_test

import (
	"testing"

	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/protobuf"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/yaml"
)

func TestFormatRegistry_RegisterAndGet(t *testing.T) {
	registry := schema.NewFormatRegistry()

	// Register protobuf format
	protoCompiler := protobuf.NewCompiler()
	protoValidator := protobuf.NewValidator()
	registry.RegisterFormat(schema.FormatProtobuf, protoCompiler, protoValidator)

	// Register YAML format
	yamlCompiler := yaml.NewCompiler()
	yamlValidator := yaml.NewValidator()
	registry.RegisterFormat(schema.FormatYaml, yamlCompiler, yamlValidator)

	// Test GetCompiler
	t.Run("GetCompiler - protobuf", func(t *testing.T) {
		compiler, err := registry.GetCompiler(schema.FormatProtobuf)
		if err != nil {
			t.Errorf("GetCompiler(FormatProtobuf) unexpected error: %v", err)
		}
		if compiler == nil {
			t.Error("GetCompiler(FormatProtobuf) returned nil compiler")
		}
	})

	t.Run("GetCompiler - yaml", func(t *testing.T) {
		compiler, err := registry.GetCompiler(schema.FormatYaml)
		if err != nil {
			t.Errorf("GetCompiler(FormatYaml) unexpected error: %v", err)
		}
		if compiler == nil {
			t.Error("GetCompiler(FormatYaml) returned nil compiler")
		}
	})

	t.Run("GetCompiler - unsupported format", func(t *testing.T) {
		_, err := registry.GetCompiler(schema.FormatJSON)
		if err == nil {
			t.Error("GetCompiler(FormatJSON) expected error, got nil")
		}
	})

	// Test GetValidator
	t.Run("GetValidator - protobuf", func(t *testing.T) {
		validator, err := registry.GetValidator(schema.FormatProtobuf)
		if err != nil {
			t.Errorf("GetValidator(FormatProtobuf) unexpected error: %v", err)
		}
		if validator == nil {
			t.Error("GetValidator(FormatProtobuf) returned nil validator")
		}
	})

	t.Run("GetValidator - yaml", func(t *testing.T) {
		validator, err := registry.GetValidator(schema.FormatYaml)
		if err != nil {
			t.Errorf("GetValidator(FormatYaml) unexpected error: %v", err)
		}
		if validator == nil {
			t.Error("GetValidator(FormatYaml) returned nil validator")
		}
	})

	t.Run("GetValidator - unsupported format", func(t *testing.T) {
		_, err := registry.GetValidator(schema.FormatJSON)
		if err == nil {
			t.Error("GetValidator(FormatJSON) expected error, got nil")
		}
	})

	// Test IsFormatSupported
	t.Run("IsFormatSupported", func(t *testing.T) {
		if !registry.IsFormatSupported(schema.FormatProtobuf) {
			t.Error("IsFormatSupported(FormatProtobuf) returned false")
		}
		if !registry.IsFormatSupported(schema.FormatYaml) {
			t.Error("IsFormatSupported(FormatYaml) returned false")
		}
		if registry.IsFormatSupported(schema.FormatJSON) {
			t.Error("IsFormatSupported(FormatJSON) returned true")
		}
	})

	// Test SupportedFormats
	t.Run("SupportedFormats", func(t *testing.T) {
		formats := registry.SupportedFormats()
		if len(formats) != 2 {
			t.Errorf("SupportedFormats() returned %d formats, want 2", len(formats))
		}

		hasProto := false
		hasYaml := false
		for _, f := range formats {
			if f == schema.FormatProtobuf {
				hasProto = true
			}
			if f == schema.FormatYaml {
				hasYaml = true
			}
		}

		if !hasProto {
			t.Error("SupportedFormats() missing FormatProtobuf")
		}
		if !hasYaml {
			t.Error("SupportedFormats() missing FormatYaml")
		}
	})
}

func TestInitializeValidator(t *testing.T) {
	t.Run("InitializeValidator - all formats", func(t *testing.T) {
		validator := schema.InitializeValidator()
		if validator == nil {
			t.Fatal("InitializeValidator() returned nil")
		}
	})

	t.Run("InitializeValidatorWithFormats - protobuf only", func(t *testing.T) {
		validator := schema.InitializeValidatorWithFormats(true, false)
		if validator == nil {
			t.Fatal("InitializeValidatorWithFormats() returned nil")
		}

		// Note: We can't access internal formatRegistry from schema_test package
		// This test is limited to basic initialization check
	})

	t.Run("InitializeValidatorWithFormats - yaml only", func(t *testing.T) {
		validator := schema.InitializeValidatorWithFormats(false, true)
		if validator == nil {
			t.Fatal("InitializeValidatorWithFormats() returned nil")
		}
	})

	t.Run("InitializeValidatorWithFormats - both formats", func(t *testing.T) {
		validator := schema.InitializeValidatorWithFormats(true, true)
		if validator == nil {
			t.Fatal("InitializeValidatorWithFormats() returned nil")
		}
	})
}
