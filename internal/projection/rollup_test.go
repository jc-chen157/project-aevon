package projection

import (
	"testing"
	"time"

	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestService_RollupToDay_Boundaries(t *testing.T) {
	svc := &Service{}

	tests := []struct {
		name         string
		start        time.Time
		end          time.Time
		aggregates   []coreagg.AggregateState
		wantBuckets  int
		wantFirstDay time.Time
	}{
		{
			name:         "end aligned to day boundary does not add extra day",
			start:        time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC),
			end:          time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC),
			aggregates:   nil,
			wantBuckets:  1,
			wantFirstDay: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:         "partial end day includes final day bucket",
			start:        time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC),
			end:          time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC),
			aggregates:   nil,
			wantBuckets:  2,
			wantFirstDay: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "value is rolled into matching day",
			start: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			end:   time.Date(2026, 2, 3, 0, 0, 0, 0, time.UTC),
			aggregates: []coreagg.AggregateState{
				{
					Operator:    coreagg.OpSum,
					Value:       decimal.NewFromInt(7),
					EventCount:  1,
					WindowStart: time.Date(2026, 2, 2, 6, 0, 0, 0, time.UTC),
				},
			},
			wantBuckets:  2,
			wantFirstDay: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := svc.rollupToDay(tc.aggregates, tc.start, tc.end)
			require.Len(t, result, tc.wantBuckets)
			require.Equal(t, tc.wantFirstDay, result[0].WindowStart)
		})
	}
}
