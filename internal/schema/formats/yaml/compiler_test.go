package yaml

import (
	"context"
	"testing"

	"github.com/aevon-lab/project-aevon/internal/schema"
)

func TestCompiler_Compile(t *testing.T) {
	compiler := NewCompiler()
	ctx := context.Background()

	tests := []struct {
		name       string
		definition string
		wantErr    bool
		errMsg     string
	}{
		{
			name: "valid shorthand - all scalar types",
			definition: `
event: test.event
version: 1
fields:
  str_field:   string
  bool_field:  bool
  int32_field: int32
  int64_field: int64
  float_field: float
  dbl_field:   double
`,
			wantErr: false,
		},
		{
			name: "valid shorthand - required fields",
			definition: `
event: test.event
version: 1
fields:
  name:   string!
  age:    int32!
  active: bool!
`,
			wantErr: false,
		},
		{
			name: "valid long form - with constraints",
			definition: `
event: test.event
version: 1
fields:
  endpoint:
    type: string!
    minLength: 1
    maxLength: 500
    pattern: "^/[a-zA-Z0-9/_-]*$"
  status_code:
    type: int32!
    min: 100
    max: 599
  method:
    type: string
    enum: [GET, POST, PUT, DELETE]
`,
			wantErr: false,
		},
		{
			name: "valid long form - required via explicit flag",
			definition: `
event: test.event
version: 1
fields:
  name:
    type: string
    required: true
`,
			wantErr: false,
		},
		{
			name: "valid mix - shorthand and long form",
			definition: `
event: test.event
version: 1
fields:
  user_id: string!
  action:  string!
  latency:
    type: float
    min: 0
`,
			wantErr: false,
		},
		{
			name: "invalid - missing event type",
			definition: `
version: 1
fields:
  name: string
`,
			wantErr: true,
			errMsg:  "event type is required",
		},
		{
			name: "invalid - version < 1",
			definition: `
event: test.event
version: 0
fields:
  name: string
`,
			wantErr: true,
			errMsg:  "version must be >= 1",
		},
		{
			name: "invalid - no fields",
			definition: `
event: test.event
version: 1
fields: {}
`,
			wantErr: true,
			errMsg:  "schema must define at least one field",
		},
		{
			name: "invalid - old format 'number' with 'kind' rejected",
			definition: `
event: test.event
version: 1
fields:
  count:
    type: number
    kind: int32
`,
			wantErr: true,
			errMsg:  "unsupported type",
		},
		{
			name: "invalid - unsupported type 'array'",
			definition: `
event: test.event
version: 1
fields:
  tags: array
`,
			wantErr: true,
			errMsg:  "unsupported type",
		},
		{
			name: "invalid - unsupported type 'map'",
			definition: `
event: test.event
version: 1
fields:
  data: map
`,
			wantErr: true,
			errMsg:  "unsupported type",
		},
		{
			name: "invalid - unknown type name",
			definition: `
event: test.event
version: 1
fields:
  value: foobar
`,
			wantErr: true,
			errMsg:  "unsupported type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &schema.Schema{
				TenantID:   "test-tenant",
				Type:       "test.event",
				Version:    1,
				Format:     schema.FormatYaml,
				Definition: []byte(tt.definition),
			}

			compiled, err := compiler.Compile(ctx, s)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Compile() expected error, got nil")
					return
				}
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("Compile() error = %v, want containing %q", err, tt.errMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("Compile() unexpected error: %v", err)
				return
			}

			if compiled == nil {
				t.Error("Compile() returned nil compiled schema")
				return
			}

			if compiled.Format != schema.FormatYaml {
				t.Errorf("Compile() format = %v, want %v", compiled.Format, schema.FormatYaml)
			}
		})
	}
}

func TestValidator_ValidateData(t *testing.T) {
	compiler := NewCompiler()
	validator := NewValidator()
	ctx := context.Background()

	schemaYAML := `
event: test.validation
version: 1
strictMode: true
fields:
  name:
    type: string!
    minLength: 1
    maxLength: 100
  email:
    type: string
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
  age:
    type: int32
    min: 0
    max: 150
  status:
    type: string
    enum: [active, inactive, pending]
  is_verified: bool
`

	s := &schema.Schema{
		TenantID:   "test-tenant",
		Type:       "test.validation",
		Version:    1,
		Format:     schema.FormatYaml,
		Definition: []byte(schemaYAML),
		StrictMode: true,
	}

	compiled, err := compiler.Compile(ctx, s)
	if err != nil {
		t.Fatalf("Failed to compile schema: %v", err)
	}

	tests := []struct {
		name    string
		data    map[string]interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid - all fields",
			data: map[string]interface{}{
				"name":        "John Doe",
				"email":       "john@example.com",
				"age":         float64(30),
				"status":      "active",
				"is_verified": true,
			},
			wantErr: false,
		},
		{
			name: "valid - required fields only",
			data: map[string]interface{}{
				"name": "Jane Doe",
			},
			wantErr: false,
		},
		{
			name: "invalid - missing required field",
			data: map[string]interface{}{
				"email": "test@example.com",
			},
			wantErr: true,
			errMsg:  "required field is missing",
		},
		{
			name: "invalid - string too short",
			data: map[string]interface{}{
				"name": "",
			},
			wantErr: true,
			errMsg:  "less than minimum",
		},
		{
			name: "invalid - string too long",
			data: map[string]interface{}{
				"name": string(make([]byte, 101)),
			},
			wantErr: true,
			errMsg:  "exceeds maximum",
		},
		{
			name: "invalid - pattern mismatch",
			data: map[string]interface{}{
				"name":  "Test User",
				"email": "not-an-email",
			},
			wantErr: true,
			errMsg:  "does not match pattern",
		},
		{
			name: "invalid - number out of range (too small)",
			data: map[string]interface{}{
				"name": "Test User",
				"age":  float64(-1),
			},
			wantErr: true,
			errMsg:  "less than minimum",
		},
		{
			name: "invalid - number out of range (too large)",
			data: map[string]interface{}{
				"name": "Test User",
				"age":  float64(200),
			},
			wantErr: true,
			errMsg:  "exceeds maximum",
		},
		{
			name: "invalid - enum mismatch",
			data: map[string]interface{}{
				"name":   "Test User",
				"status": "unknown",
			},
			wantErr: true,
			errMsg:  "not in enum",
		},
		{
			name: "invalid - wrong type (string instead of number)",
			data: map[string]interface{}{
				"name": "Test User",
				"age":  "thirty",
			},
			wantErr: true,
			errMsg:  "expected number",
		},
		{
			name: "invalid - wrong type (number instead of boolean)",
			data: map[string]interface{}{
				"name":        "Test User",
				"is_verified": 1,
			},
			wantErr: true,
			errMsg:  "expected boolean",
		},
		{
			name: "invalid - unknown field in strict mode",
			data: map[string]interface{}{
				"name":          "Test User",
				"unknown_field": "value",
			},
			wantErr: true,
			errMsg:  "unknown field",
		},
		{
			name: "invalid - array value for scalar field rejected",
			data: map[string]interface{}{
				"name": []interface{}{"not", "a", "string"},
			},
			wantErr: true,
			errMsg:  "expected string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateData(ctx, compiled, tt.data)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateData() expected error, got nil")
					return
				}
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateData() error = %v, want containing %q", err, tt.errMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("ValidateData() unexpected error: %v", err)
			}
		})
	}
}

func TestValidator_NumberOverflow(t *testing.T) {
	compiler := NewCompiler()
	validator := NewValidator()
	ctx := context.Background()

	tests := []struct {
		name       string
		kind       string
		value      interface{}
		wantErr    bool
		errContain string
	}{
		// int32 tests
		{
			name:    "int32 - valid min",
			kind:    "int32",
			value:   float64(-2147483648),
			wantErr: false,
		},
		{
			name:    "int32 - valid max",
			kind:    "int32",
			value:   float64(2147483647),
			wantErr: false,
		},
		{
			name:       "int32 - overflow (too large)",
			kind:       "int32",
			value:      float64(3000000000), // 3 billion
			wantErr:    true,
			errContain: "out of range for int32",
		},
		{
			name:       "int32 - underflow (too small)",
			kind:       "int32",
			value:      float64(-3000000000),
			wantErr:    true,
			errContain: "out of range for int32",
		},
		{
			name:       "int32 - reject fractional",
			kind:       "int32",
			value:      float64(123.45),
			wantErr:    true,
			errContain: "fractional part",
		},

		// int64 tests
		{
			name:    "int64 - valid large number",
			kind:    "int64",
			value:   float64(9007199254740991), // Max safe integer in float64
			wantErr: false,
		},
		{
			name:       "int64 - reject fractional",
			kind:       "int64",
			value:      float64(999.999),
			wantErr:    true,
			errContain: "fractional part",
		},

		// float tests
		{
			name:    "float - valid decimal",
			kind:    "float",
			value:   float64(123.456),
			wantErr: false,
		},
		{
			name:    "float - valid integer",
			kind:    "float",
			value:   float64(12345),
			wantErr: false,
		},

		// double tests
		{
			name:    "double - valid decimal",
			kind:    "double",
			value:   float64(999999.999999),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaYAML := `
event: test.overflow
version: 1
fields:
  value: ` + tt.kind + `
`
			s := &schema.Schema{
				TenantID:   "test-tenant",
				Type:       "test.overflow",
				Version:    1,
				Format:     schema.FormatYaml,
				Definition: []byte(schemaYAML),
			}

			compiled, err := compiler.Compile(ctx, s)
			if err != nil {
				t.Fatalf("Failed to compile schema: %v", err)
			}

			data := map[string]interface{}{
				"value": tt.value,
			}

			err = validator.ValidateData(ctx, compiled, data)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateData() expected error for value %v, got nil", tt.value)
					return
				}
				if tt.errContain != "" && !contains(err.Error(), tt.errContain) {
					t.Errorf("ValidateData() error = %v, want containing %q", err, tt.errContain)
				}
				return
			}

			if err != nil {
				t.Errorf("ValidateData() unexpected error: %v", err)
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && containsImpl(s, substr)))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
