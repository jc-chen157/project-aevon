package v1

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEvent_Validation(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name    string
		event   Event
		wantErr bool
		checkFn func(*testing.T, *Event) // Optional validation after Validate()
	}{
		{
			name: "valid event with all fields",
			event: Event{
				ID:          "evt_123",
				TenantID:    "tenant_abc",
				PrincipalID: "user:alice@example.com",
				Type:        "test.event",
				OccurredAt:  now,
			},
			wantErr: false,
		},
		{
			name: "valid event - tenant_id defaults to 'default'",
			event: Event{
				ID:          "evt_123",
				PrincipalID: "user:bob@example.com",
				Type:        "test.event",
				OccurredAt:  now,
			},
			wantErr: false,
			checkFn: func(t *testing.T, e *Event) {
				if e.TenantID != "default" {
					t.Errorf("TenantID should default to 'default', got %q", e.TenantID)
				}
			},
		},
		{
			name: "valid event - explicit tenant_id preserved",
			event: Event{
				ID:          "evt_456",
				TenantID:    "customer-acme",
				PrincipalID: "account:prod-123",
				Type:        "test.event",
				OccurredAt:  now,
			},
			wantErr: false,
			checkFn: func(t *testing.T, e *Event) {
				if e.TenantID != "customer-acme" {
					t.Errorf("TenantID should be preserved, got %q", e.TenantID)
				}
			},
		},
		{
			name: "missing id",
			event: Event{
				TenantID:    "tenant_abc",
				PrincipalID: "user:alice",
				Type:        "test.event",
				OccurredAt:  now,
			},
			wantErr: true,
		},
		{
			name: "missing principal_id",
			event: Event{
				ID:         "evt_123",
				TenantID:   "tenant_abc",
				Type:       "test.event",
				OccurredAt: now,
			},
			wantErr: true,
		},
		{
			name: "missing type",
			event: Event{
				ID:          "evt_123",
				TenantID:    "tenant_abc",
				PrincipalID: "user:alice",
				OccurredAt:  now,
			},
			wantErr: true,
		},
		{
			name: "missing occurred_at",
			event: Event{
				ID:          "evt_123",
				TenantID:    "tenant_abc",
				PrincipalID: "user:alice",
				Type:        "test.event",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Event.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Run additional checks if provided
			if tt.checkFn != nil && err == nil {
				tt.checkFn(t, &tt.event)
			}
		})
	}
}

func TestEvent_PrincipalIDRequired(t *testing.T) {
	evt := Event{
		ID:         "evt_123",
		Type:       "test.event",
		OccurredAt: time.Now(),
		// Missing PrincipalID
	}

	err := evt.Validate()
	if err == nil {
		t.Fatal("Expected validation error for missing principal_id")
	}

	expectedMsg := "principal_id is required"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message %q, got %q", expectedMsg, err.Error())
	}
}

func TestEvent_TenantIDDefaults(t *testing.T) {
	evt := Event{
		ID:          "evt_123",
		PrincipalID: "user:alice",
		Type:        "test.event",
		OccurredAt:  time.Now(),
		// TenantID omitted
	}

	if err := evt.Validate(); err != nil {
		t.Fatalf("Validation should succeed with default tenant_id: %v", err)
	}

	if evt.TenantID != "default" {
		t.Errorf("TenantID should default to 'default', got %q", evt.TenantID)
	}
}

func TestEvent_JSONMarshaling(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2024-01-01T12:00:00Z")
	evt := Event{
		ID:          "evt_123",
		TenantID:    "tenant_abc",
		PrincipalID: "user:alice@example.com",
		Type:        "api.request",
		OccurredAt:  now,
		Metadata:    map[string]string{"region": "us-east-1"},
		Data:        map[string]interface{}{"path": "/v1/test", "latency": 100},
	}

	// Marshal
	bytes, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal
	var unmarshaled Event
	if err := json.Unmarshal(bytes, &unmarshaled); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Verify fields
	if unmarshaled.ID != evt.ID {
		t.Errorf("ID mismatch: got %v, want %v", unmarshaled.ID, evt.ID)
	}
	if unmarshaled.TenantID != evt.TenantID {
		t.Errorf("TenantID mismatch: got %v, want %v", unmarshaled.TenantID, evt.TenantID)
	}
	if unmarshaled.PrincipalID != evt.PrincipalID {
		t.Errorf("PrincipalID mismatch: got %v, want %v", unmarshaled.PrincipalID, evt.PrincipalID)
	}
	if unmarshaled.Metadata["region"] != "us-east-1" {
		t.Errorf("Metadata mismatch: got %v", unmarshaled.Metadata)
	}
	if path, ok := unmarshaled.Data["path"].(string); !ok || path != "/v1/test" {
		t.Errorf("Data payload mismatch or type loss")
	}
}

func TestEvent_JSONMarshalingWithDefaults(t *testing.T) {
	// Test that unmarshaling JSON without tenant_id works after validation
	jsonData := `{
		"id": "evt_789",
		"principal_id": "apikey:prod-key-456",
		"type": "webhook.received",
		"occurred_at": "2024-01-01T12:00:00Z",
		"data": {"source": "stripe"}
	}`

	var evt Event
	if err := json.Unmarshal([]byte(jsonData), &evt); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// Validate should apply defaults
	if err := evt.Validate(); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	if evt.TenantID != "default" {
		t.Errorf("TenantID should default to 'default', got %q", evt.TenantID)
	}
	if evt.PrincipalID != "apikey:prod-key-456" {
		t.Errorf("PrincipalID mismatch: got %q", evt.PrincipalID)
	}
}

func TestEvent_PrincipalIDFormats(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		name        string
		principalID string
		shouldPass  bool
	}{
		{"user email", "user:alice@example.com", true},
		{"account id", "account:12345", true},
		{"api key", "apikey:prod-key-789", true},
		{"service account", "service:payment-processor", true},
		{"device id", "device:iphone-xyz", true},
		{"empty principal", "", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			evt := Event{
				ID:          "evt_test",
				PrincipalID: tc.principalID,
				Type:        "test.event",
				OccurredAt:  now,
			}

			err := evt.Validate()
			if tc.shouldPass && err != nil {
				t.Errorf("Expected %q to be valid, got error: %v", tc.principalID, err)
			}
			if !tc.shouldPass && err == nil {
				t.Errorf("Expected %q to be invalid, but validation passed", tc.principalID)
			}
		})
	}
}
