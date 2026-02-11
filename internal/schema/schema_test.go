package schema_test

import (
	"context"
	"testing"

	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/protobuf"
	"github.com/aevon-lab/project-aevon/internal/schema/storage"
)

func TestRegistry_Register(t *testing.T) {
	repo := storage.NewMemoryRepository()
	reg := schema.NewRegistry(repo)
	ctx := context.Background()

	tests := []struct {
		name       string
		tenantID   string
		eventType  string
		version    int
		definition []byte
		wantErr    bool
		errMsg     string
	}{
		{
			name:       "valid schema",
			tenantID:   "tenant_123",
			eventType:  "api.request",
			version:    1,
			definition: []byte(`syntax = "proto3"; message ApiRequest { string endpoint = 1; }`),
			wantErr:    false,
		},
		{
			name:       "missing tenant_id",
			tenantID:   "",
			eventType:  "api.request",
			version:    1,
			definition: []byte(`syntax = "proto3"; message Test { }`),
			wantErr:    true,
			errMsg:     "tenant_id is required",
		},
		{
			name:       "missing type",
			tenantID:   "tenant_123",
			eventType:  "",
			version:    1,
			definition: []byte(`syntax = "proto3"; message Test { }`),
			wantErr:    true,
			errMsg:     "type is required",
		},
		{
			name:       "invalid version",
			tenantID:   "tenant_123",
			eventType:  "api.request",
			version:    0,
			definition: []byte(`syntax = "proto3"; message Test { }`),
			wantErr:    true,
			errMsg:     "version must be >= 1",
		},
		{
			name:       "empty definition",
			tenantID:   "tenant_123",
			eventType:  "api.request",
			version:    1,
			definition: []byte{},
			wantErr:    true,
			errMsg:     "definition is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := reg.Register(ctx, tt.tenantID, tt.eventType, tt.version, schema.FormatProtobuf, tt.definition, true)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Register() expected error, got nil")
				} else if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("Register() error = %v, want %v", err.Error(), tt.errMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("Register() unexpected error: %v", err)
				return
			}

			if s.ID == "" {
				t.Error("Register() schema.ID should not be empty")
			}
			if s.TenantID != tt.tenantID {
				t.Errorf("Register() schema.TenantID = %v, want %v", s.TenantID, tt.tenantID)
			}
			if s.Type != tt.eventType {
				t.Errorf("Register() schema.Type = %v, want %v", s.Type, tt.eventType)
			}
			if s.State != schema.StateActive {
				t.Errorf("Register() schema.State = %v, want %v", s.State, schema.StateActive)
			}
		})
	}
}

func TestRegistry_Get_HybridLookup(t *testing.T) {
	repo := storage.NewMemoryRepository()
	reg := schema.NewRegistry(repo)
	ctx := context.Background()

	// Register a platform schema
	platformProto := []byte(`syntax = "proto3"; message Usage { string resource_id = 1; double quantity = 2; }`)
	_, err := reg.Register(ctx, schema.PlatformTenantID, "aevon.usage", 1, schema.FormatProtobuf, platformProto, true)
	if err != nil {
		t.Fatalf("Failed to register platform schema: %v", err)
	}

	// Register a tenant-specific schema
	tenantProto := []byte(`syntax = "proto3"; message ApiRequest { string endpoint = 1; }`)
	_, err = reg.Register(ctx, "tenant_123", "api.request", 1, schema.FormatProtobuf, tenantProto, true)
	if err != nil {
		t.Fatalf("Failed to register tenant schema: %v", err)
	}

	tests := []struct {
		name         string
		tenantID     string
		eventType    string
		version      int
		wantErr      bool
		wantTenantID string
	}{
		{
			name:         "tenant-specific schema lookup",
			tenantID:     "tenant_123",
			eventType:    "api.request",
			version:      1,
			wantErr:      false,
			wantTenantID: "tenant_123",
		},
		{
			name:         "fallback to platform schema",
			tenantID:     "tenant_123",
			eventType:    "aevon.usage",
			version:      1,
			wantErr:      false,
			wantTenantID: schema.PlatformTenantID,
		},
		{
			name:      "schema not found",
			tenantID:  "tenant_123",
			eventType: "nonexistent",
			version:   1,
			wantErr:   true,
		},
		{
			name:      "version not found",
			tenantID:  "tenant_123",
			eventType: "api.request",
			version:   2,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := reg.Get(ctx, tt.tenantID, tt.eventType, tt.version)

			if tt.wantErr {
				if err == nil {
					t.Error("Get() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Get() unexpected error: %v", err)
				return
			}

			if s.TenantID != tt.wantTenantID {
				t.Errorf("Get() schema.TenantID = %v, want %v", s.TenantID, tt.wantTenantID)
			}
		})
	}
}

func TestValidator_ValidateData(t *testing.T) {
	v := schema.InitializeValidator()
	v.RegisterFormat(schema.FormatProtobuf, protobuf.NewCompiler(), protobuf.NewValidator())
	ctx := context.Background()

	// Sample proto schema
	proto := []byte(`
syntax = "proto3";

message ApiRequest {
  string endpoint = 1;
  string method = 2;
  int32 status_code = 3;
  int64 latency_ms = 4;
}
`)

	s := &schema.Schema{
		ID:         "test-id",
		TenantID:   "tenant_123",
		Type:       "api.request",
		Version:    1,
		Format:     schema.FormatProtobuf,
		Definition: proto,
		StrictMode: true,
	}

	tests := []struct {
		name    string
		data    map[string]interface{}
		wantErr bool
	}{
		{
			name: "valid data - all fields",
			data: map[string]interface{}{
				"endpoint":    "/v1/users",
				"method":      "POST",
				"status_code": float64(201),
				"latency_ms":  float64(45),
			},
			wantErr: false,
		},
		{
			name: "valid data - partial fields",
			data: map[string]interface{}{
				"endpoint": "/v1/test",
				"method":   "GET",
			},
			wantErr: false,
		},
		{
			name:    "valid data - empty",
			data:    map[string]interface{}{},
			wantErr: false,
		},
		{
			name: "invalid - wrong type for string field",
			data: map[string]interface{}{
				"endpoint": 123,
			},
			wantErr: true,
		},
		{
			name: "invalid - unknown field in strict mode",
			data: map[string]interface{}{
				"endpoint":    "/v1/test",
				"extra_field": "not in schema",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.ValidateData(ctx, s, tt.data)

			if tt.wantErr {
				if err == nil {
					t.Error("ValidateData() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("ValidateData() unexpected error: %v", err)
			}
		})
	}
}

func TestComputeFingerprint(t *testing.T) {
	data := []byte("test data")
	fp1 := schema.ComputeFingerprint(data)
	fp2 := schema.ComputeFingerprint(data)

	if fp1 != fp2 {
		t.Errorf("ComputeFingerprint() not deterministic: %v != %v", fp1, fp2)
	}

	if len(fp1) != 64 { // SHA-256 hex is 64 chars
		t.Errorf("ComputeFingerprint() length = %d, want 64", len(fp1))
	}

	different := []byte("different data")
	fp3 := schema.ComputeFingerprint(different)
	if fp1 == fp3 {
		t.Error("ComputeFingerprint() should produce different hashes for different data")
	}
}
