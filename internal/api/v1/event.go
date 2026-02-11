package v1

import (
	"fmt"
	"time"
)

// Event is the atomic unit of the system.
// It separates the "Envelope" (System Attributes) from the "Letter" (Data).
type Event struct {
	// --- System Attributes (The Envelope) ---

	// ID is the unique immutable identifier provided by the client.
	// It MUST be unique per PrincipalID to enforce idempotency.
	ID string `json:"id"`

	// PrincipalID identifies the actor/principal that generated this event.
	// Examples: "user:alice@example.com", "account:123", "apikey:prod-key-789"
	// This is the primary dimension for usage aggregation and attribution.
	// This field is REQUIRED and has no default value.
	PrincipalID string `json:"principal_id"`

	// Metadata is a generic key-value store for context (e.g., source, trace_id, region).
	// This allows for flexible stamping of side-channel data.
	Metadata map[string]string `json:"metadata,omitempty"`

	// Type is the domain-specific event name (e.g., "api.request", "invoice.created").
	// This acts as the key for the Schema Registry lookup.
	Type string `json:"type"`

	// SchemaVersion allows for evolving the 'Data' structure over time without breaking consumers.
	SchemaVersion int `json:"schema_version"`

	// OccurredAt is when the event indeed happened in the real world (client-side clock).
	// This distinguishes it from IngestedAt (server-side clock).
	OccurredAt time.Time `json:"occurred_at"`

	// IngestedAt is when Aevon received the event (Audit trail).
	// This should be set by the Ingestion Service, not the user.
	IngestedAt time.Time `json:"ingested_at"`

	// IngestSeq is a monotonic sequence number assigned on ingestion.
	// This provides strict total ordering for recovery pagination.
	// Set by database (BIGSERIAL), not exposed in public API.
	IngestSeq int64 `json:"-"`

	// --- User Payload (The Letter) ---

	// Data is the domain-specific payload.
	// In v0.1, we accept dynamic JSON.
	// Future: This will be validated against a Schema Registry (Protobuf/Avro/JSONSchema) based on (Type, SchemaVersion).
	Data map[string]interface{} `json:"data"`
}

// Validate ensures the event has all required system attributes.
func (e *Event) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("id is required")
	}

	if e.PrincipalID == "" {
		return fmt.Errorf("principal_id is required")
	}

	if e.Type == "" {
		return fmt.Errorf("type is required")
	}

	if e.OccurredAt.IsZero() {
		return fmt.Errorf("occurred_at is required")
	}

	return nil
}
