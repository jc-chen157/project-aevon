package aggregation

import (
	"time"

	"github.com/shopspring/decimal"
)

// Supported aggregation operators for MVP.
// avg and last are deferred — they require composite state (sum+count, value+timestamp).
const (
	OpCount = "count"
	OpSum   = "sum"
	OpMin   = "min"
	OpMax   = "max"
)

// AggregateKey uniquely identifies a pre-aggregate bucket.
// Partition-scoped from day one: PartitionID is always present,
// even when running as a single instance.
type AggregateKey struct {
	PartitionID int
	PrincipalID string
	RuleName    string
	BucketSize  string    // e.g. "1m", "10m", "1h"
	WindowStart time.Time // truncated to bucket boundary
}

// AggregateState holds the current materialized value of a pre-aggregate.
type AggregateState struct {
	Operator        string          // count, sum, min, max
	Value           decimal.Decimal // the aggregate value (exact arithmetic)
	EventCount      int64           // monotonically increasing; idempotency marker for upsert
	LastEventID     string          // most recent event ID that updated this aggregate
	RuleFingerprint string          // SHA-256 of the rule definition; staleness detection at query time
	WindowStart     time.Time       // bucket timestamp (truncated to 1-min boundary)
	UpdatedAt       time.Time       // last update timestamp
}
