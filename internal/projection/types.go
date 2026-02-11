package projection

import (
	"time"

	"github.com/shopspring/decimal"
)

// AggregateQueryRequest represents the query parameters for fetching aggregates.
type AggregateQueryRequest struct {
	TenantID    string    `uri:"tenant_id" binding:"required"`
	PrincipalID string    `uri:"principal_id" binding:"required"`
	Rule        string    `form:"rule" binding:"required"`
	Start       time.Time `form:"start" binding:"required" time_format:"2006-01-02T15:04:05Z07:00"`
	End         time.Time `form:"end" binding:"required" time_format:"2006-01-02T15:04:05Z07:00"`
	Granularity string    `form:"granularity"` // default: "total"
}

// AggregateValue represents a single aggregate data point in the response.
type AggregateValue struct {
	WindowStart time.Time       `json:"window_start"`
	WindowEnd   time.Time       `json:"window_end"`
	Value       decimal.Decimal `json:"value"`
	EventCount  int64           `json:"event_count"`
}

// AggregateQueryResponse represents the response for an aggregate query.
type AggregateQueryResponse struct {
	TenantID         string           `json:"tenant_id"`
	PrincipalID      string           `json:"principal_id"`
	Rule             string           `json:"rule"`
	Operator         string           `json:"operator"`
	Start            time.Time        `json:"start"`
	End              time.Time        `json:"end"`
	Granularity      string           `json:"granularity"`
	DataThrough      time.Time        `json:"data_through"`
	StalenessSeconds int              `json:"staleness_seconds"`
	Values           []AggregateValue `json:"values"`
}
