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
				PrincipalID: "user:alice@example.com",
				Type:        "test.event",
				OccurredAt:  now,
			},
			wantErr: false,
		},
		{
			name: "missing id",
			event: Event{
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
				Type:       "test.event",
				OccurredAt: now,
			},
			wantErr: true,
		},
		{
			name: "missing type",
			event: Event{
				ID:          "evt_123",
				PrincipalID: "user:alice",
				OccurredAt:  now,
			},
			wantErr: true,
		},
		{
			name: "missing occurred_at",
			event: Event{
				ID:          "evt_123",
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

func TestEvent_JSONMarshaling(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2024-01-01T12:00:00Z")
	evt := Event{
		ID:          "evt_123",
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
