package schema_test

import (
	"context"
	"testing"

	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/protobuf"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/yaml"
)

// TestFormatEquivalence verifies that YAML and Protobuf produce equivalent validation results
func TestFormatEquivalence(t *testing.T) {
	ctx := context.Background()
	validator := schema.InitializeValidator()
	validator.RegisterFormat(schema.FormatProtobuf, protobuf.NewCompiler(), protobuf.NewValidator())
	validator.RegisterFormat(schema.FormatYaml, yaml.NewCompiler(), yaml.NewValidator())

	// Define equivalent schemas in both formats
	protoSchema := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "user.event",
		Version:  1,
		Format:   schema.FormatProtobuf,
		Definition: []byte(`
syntax = "proto3";

message UserEvent {
  string user_id = 1;
  string action = 2;
  int32 status_code = 3;
}
`),
		Fingerprint: schema.ComputeFingerprint([]byte("proto-v1")),
		StrictMode:  true,
	}

	yamlSchema := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "user.event",
		Version:  2, // Different version to avoid cache collision
		Format:   schema.FormatYaml,
		Definition: []byte(`
event: user.event
version: 2
strictMode: true
fields:
  user_id:     string
  action:      string
  status_code: int32
`),
		Fingerprint: schema.ComputeFingerprint([]byte("yaml-v2")),
		StrictMode:  true,
	}

	testCases := []struct {
		name    string
		data    map[string]interface{}
		wantErr bool
	}{
		{
			name: "valid data - all fields",
			data: map[string]interface{}{
				"user_id":     "user123",
				"action":      "login",
				"status_code": float64(200),
			},
			wantErr: false,
		},
		{
			name: "valid data - partial fields",
			data: map[string]interface{}{
				"user_id": "user456",
			},
			wantErr: false,
		},
		{
			name: "invalid - unknown field (strict mode)",
			data: map[string]interface{}{
				"user_id":       "user789",
				"unknown_field": "value",
			},
			wantErr: true,
		},
		{
			name: "invalid - wrong type",
			data: map[string]interface{}{
				"user_id":     "user999",
				"status_code": "not a number",
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Validate with protobuf schema
			protoErr := validator.ValidateData(ctx, protoSchema, tc.data)

			// Validate with YAML schema
			yamlErr := validator.ValidateData(ctx, yamlSchema, tc.data)

			// Both should have the same success/failure result
			protoFailed := protoErr != nil
			yamlFailed := yamlErr != nil

			if protoFailed != yamlFailed {
				t.Errorf("Format equivalence broken:\n  Proto error: %v\n  YAML error: %v", protoErr, yamlErr)
			}

			if protoFailed != tc.wantErr {
				t.Errorf("Unexpected validation result: wantErr=%v, protoErr=%v, yamlErr=%v",
					tc.wantErr, protoErr, yamlErr)
			}
		})
	}
}

// TestCacheFingerprint verifies that cache uses fingerprint to prevent collisions
func TestCacheFingerprint(t *testing.T) {
	ctx := context.Background()
	validator := schema.InitializeValidator()
	validator.RegisterFormat(schema.FormatProtobuf, protobuf.NewCompiler(), protobuf.NewValidator())
	validator.RegisterFormat(schema.FormatYaml, yaml.NewCompiler(), yaml.NewValidator())

	// Create two schemas with same tenant/type/version but different formats
	// This simulates replacing a .proto with a .yaml file
	version := 1

	yamlSchema := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "cache.test",
		Version:  version,
		Format:   schema.FormatYaml,
		Definition: []byte(`
event: cache.test
version: 1
fields:
  required_field: string!
  optional_field: string
`),
		Fingerprint: schema.ComputeFingerprint([]byte("yaml-fingerprint")),
		StrictMode:  true,
	}

	protoSchema := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "cache.test",
		Version:  version,
		Format:   schema.FormatProtobuf,
		Definition: []byte(`
syntax = "proto3";
message CacheTest {
  string required_field = 1;
  string optional_field = 2;
  string extra_field = 3;
}
`),
		Fingerprint: schema.ComputeFingerprint([]byte("proto-fingerprint")),
		StrictMode:  true,
	}

	// Data with only required field
	dataMinimal := map[string]interface{}{
		"required_field": "value",
	}

	// Data with extra field (allowed in proto, rejected in yaml strict mode)
	dataWithExtra := map[string]interface{}{
		"required_field": "value",
		"extra_field":    "extra",
	}

	// Validate with YAML schema first
	err := validator.ValidateData(ctx, yamlSchema, dataMinimal)
	if err != nil {
		t.Fatalf("YAML validation (minimal) failed: %v", err)
	}

	err = validator.ValidateData(ctx, yamlSchema, dataWithExtra)
	if err == nil {
		t.Error("YAML validation should reject extra_field in strict mode")
	}

	// Now validate with proto schema - should use different cache entry
	err = validator.ValidateData(ctx, protoSchema, dataWithExtra)
	if err != nil {
		t.Fatalf("Proto validation failed: %v", err)
	}

	// Verify YAML schema still rejects extra field (cache not corrupted)
	err = validator.ValidateData(ctx, yamlSchema, dataWithExtra)
	if err == nil {
		t.Error("YAML validation should still reject extra_field (cache collision detected)")
	}
}

// TestConcurrentCompilation verifies singleflight deduplication
func TestConcurrentCompilation(t *testing.T) {
	ctx := context.Background()
	validator := schema.InitializeValidator()
	validator.RegisterFormat(schema.FormatYaml, yaml.NewCompiler(), yaml.NewValidator())

	s := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "concurrent.test",
		Version:  1,
		Format:   schema.FormatYaml,
		Definition: []byte(`
event: concurrent.test
version: 1
fields:
  data: string
`),
		Fingerprint: schema.ComputeFingerprint([]byte("concurrent-test")),
		StrictMode:  true,
	}

	data := map[string]interface{}{
		"data": "test",
	}

	// Simulate concurrent validation requests
	const numGoroutines = 50
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			err := validator.ValidateData(ctx, s, data)
			errChan <- err
		}()
	}

	// Collect results
	for i := 0; i < numGoroutines; i++ {
		err := <-errChan
		if err != nil {
			t.Errorf("Concurrent validation %d failed: %v", i, err)
		}
	}

	// If singleflight works correctly, compilation should happen only once
	// We can't directly verify this without exposing internal state,
	// but the test ensures correctness under concurrent load
}

// TestSchemaEvolution tests version migration scenarios
func TestSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	validator := schema.InitializeValidator()
	validator.RegisterFormat(schema.FormatYaml, yaml.NewCompiler(), yaml.NewValidator())

	// V1: Original schema
	v1Schema := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "evolving.event",
		Version:  1,
		Format:   schema.FormatYaml,
		Definition: []byte(`
event: evolving.event
version: 1
fields:
  id:   string!
  name: string!
`),
		Fingerprint: schema.ComputeFingerprint([]byte("v1")),
		StrictMode:  true,
	}

	// V2: Added optional field, removed required constraint from name
	v2Schema := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "evolving.event",
		Version:  2,
		Format:   schema.FormatYaml,
		Definition: []byte(`
event: evolving.event
version: 2
fields:
  id:    string!
  name:  string
  email: string
`),
		Fingerprint: schema.ComputeFingerprint([]byte("v2")),
		StrictMode:  true,
	}

	// Data that's valid for V1
	v1Data := map[string]interface{}{
		"id":   "123",
		"name": "Test User",
	}

	// Data that's valid for V2 but invalid for V1 (missing name)
	v2Data := map[string]interface{}{
		"id":    "456",
		"email": "test@example.com",
	}

	// Test V1 schema
	t.Run("V1 validates V1 data", func(t *testing.T) {
		err := validator.ValidateData(ctx, v1Schema, v1Data)
		if err != nil {
			t.Errorf("V1 schema rejected V1 data: %v", err)
		}
	})

	t.Run("V1 rejects V2 data", func(t *testing.T) {
		err := validator.ValidateData(ctx, v1Schema, v2Data)
		if err == nil {
			t.Error("V1 schema should reject data missing required 'name' field")
		}
	})

	// Test V2 schema
	t.Run("V2 validates V1 data (backward compatible)", func(t *testing.T) {
		err := validator.ValidateData(ctx, v2Schema, v1Data)
		if err != nil {
			t.Errorf("V2 schema rejected V1 data: %v", err)
		}
	})

	t.Run("V2 validates V2 data", func(t *testing.T) {
		err := validator.ValidateData(ctx, v2Schema, v2Data)
		if err != nil {
			t.Errorf("V2 schema rejected V2 data: %v", err)
		}
	})
}

// TestErrorFormat verifies that error messages include format for debugging
func TestErrorFormat(t *testing.T) {
	ctx := context.Background()
	validator := schema.InitializeValidator()
	validator.RegisterFormat(schema.FormatYaml, yaml.NewCompiler(), yaml.NewValidator())

	yamlSchema := &schema.Schema{
		TenantID: "test-tenant",
		Type:     "error.test",
		Version:  1,
		Format:   schema.FormatYaml,
		Definition: []byte(`
event: error.test
version: 1
strictMode: true
fields:
  required_field: string!
`),
		Fingerprint: schema.ComputeFingerprint([]byte("error-test")),
		StrictMode:  true,
	}

	// Test missing required field
	t.Run("Missing required field includes format", func(t *testing.T) {
		data := map[string]interface{}{}
		err := validator.ValidateData(ctx, yamlSchema, data)

		if err == nil {
			t.Fatal("Expected validation error")
		}

		// Check if error is ValidationError and has format
		if ve, ok := err.(*schema.ValidationError); ok {
			if ve.Format != string(schema.FormatYaml) {
				t.Errorf("ValidationError.Format = %q, want %q", ve.Format, schema.FormatYaml)
			}
		} else if multiErr, ok := err.(*schema.MultiValidationError); ok {
			if len(multiErr.Errors) > 0 {
				if multiErr.Errors[0].Format != string(schema.FormatYaml) {
					t.Errorf("ValidationError.Format = %q, want %q",
						multiErr.Errors[0].Format, schema.FormatYaml)
				}
			}
		} else {
			t.Errorf("Expected ValidationError or MultiValidationError, got %T", err)
		}
	})
}
