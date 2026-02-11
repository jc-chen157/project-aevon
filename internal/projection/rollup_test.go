package projection

import (
	"testing"
	"time"

	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestService_RollupTotal(t *testing.T) {
	svc := &Service{}
	start := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)

	tests := []struct {
		name       string
		aggregates []coreagg.AggregateState
		wantValue  decimal.Decimal
		wantEvents int64
	}{
		{
			name:       "empty returns zero window",
			aggregates: nil,
			wantValue:  decimal.Zero,
			wantEvents: 0,
		},
		{
			name: "sum operator adds values",
			aggregates: []coreagg.AggregateState{
				{Operator: coreagg.OpSum, Value: decimal.NewFromInt(3), EventCount: 2},
				{Operator: coreagg.OpSum, Value: decimal.NewFromInt(5), EventCount: 4},
			},
			wantValue:  decimal.NewFromInt(8),
			wantEvents: 6,
		},
		{
			name: "count operator adds values",
			aggregates: []coreagg.AggregateState{
				{Operator: coreagg.OpCount, Value: decimal.NewFromInt(9), EventCount: 9},
				{Operator: coreagg.OpCount, Value: decimal.NewFromInt(1), EventCount: 1},
			},
			wantValue:  decimal.NewFromInt(10),
			wantEvents: 10,
		},
		{
			name: "min operator picks global minimum",
			aggregates: []coreagg.AggregateState{
				{Operator: coreagg.OpMin, Value: decimal.NewFromInt(9), EventCount: 1},
				{Operator: coreagg.OpMin, Value: decimal.NewFromInt(4), EventCount: 2},
				{Operator: coreagg.OpMin, Value: decimal.NewFromInt(7), EventCount: 3},
			},
			wantValue:  decimal.NewFromInt(4),
			wantEvents: 6,
		},
		{
			name: "max operator picks global maximum",
			aggregates: []coreagg.AggregateState{
				{Operator: coreagg.OpMax, Value: decimal.NewFromInt(9), EventCount: 1},
				{Operator: coreagg.OpMax, Value: decimal.NewFromInt(4), EventCount: 2},
				{Operator: coreagg.OpMax, Value: decimal.NewFromInt(12), EventCount: 3},
			},
			wantValue:  decimal.NewFromInt(12),
			wantEvents: 6,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			values := svc.rollupTotal(tc.aggregates, start, end)
			require.Len(t, values, 1)
			require.Equal(t, start, values[0].WindowStart)
			require.Equal(t, end, values[0].WindowEnd)
			require.True(t, tc.wantValue.Equal(values[0].Value))
			require.Equal(t, tc.wantEvents, values[0].EventCount)
		})
	}
}

func TestService_ConvertToValues(t *testing.T) {
	svc := &Service{}
	base := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	aggregates := []coreagg.AggregateState{
		{WindowStart: base, Value: decimal.NewFromInt(3), EventCount: 2},
	}

	t.Run("uses default one minute for non-positive duration", func(t *testing.T) {
		values := svc.convertToValues(aggregates, 0)
		require.Len(t, values, 1)
		require.Equal(t, base.Add(time.Minute), values[0].WindowEnd)
	})

	t.Run("uses provided duration", func(t *testing.T) {
		values := svc.convertToValues(aggregates, time.Hour)
		require.Len(t, values, 1)
		require.Equal(t, base.Add(time.Hour), values[0].WindowEnd)
	})
}

func TestService_RollupToHour(t *testing.T) {
	svc := &Service{}

	t.Run("empty aggregates produce hourly zero buckets", func(t *testing.T) {
		start := time.Date(2026, 2, 1, 10, 15, 0, 0, time.UTC)
		end := time.Date(2026, 2, 1, 13, 0, 0, 0, time.UTC)

		result := svc.rollupToHour(nil, start, end)
		require.Len(t, result, 3)
		require.Equal(t, time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC), result[0].WindowStart)
		require.Equal(t, decimal.Zero, result[1].Value)
		require.Equal(t, int64(0), result[1].EventCount)
	})

	t.Run("sum operator aggregates per hour and preserves empty bucket", func(t *testing.T) {
		start := time.Date(2026, 2, 1, 10, 15, 0, 0, time.UTC)
		end := time.Date(2026, 2, 1, 13, 0, 0, 0, time.UTC)
		aggregates := []coreagg.AggregateState{
			{Operator: coreagg.OpSum, WindowStart: time.Date(2026, 2, 1, 10, 5, 0, 0, time.UTC), Value: decimal.NewFromInt(2), EventCount: 2},
			{Operator: coreagg.OpSum, WindowStart: time.Date(2026, 2, 1, 10, 55, 0, 0, time.UTC), Value: decimal.NewFromInt(3), EventCount: 1},
			{Operator: coreagg.OpSum, WindowStart: time.Date(2026, 2, 1, 12, 10, 0, 0, time.UTC), Value: decimal.NewFromInt(4), EventCount: 4},
		}

		result := svc.rollupToHour(aggregates, start, end)
		require.Len(t, result, 3)
		require.Equal(t, "5", result[0].Value.String())
		require.Equal(t, int64(3), result[0].EventCount)
		require.Equal(t, "0", result[1].Value.String())
		require.Equal(t, int64(0), result[1].EventCount)
		require.Equal(t, "4", result[2].Value.String())
		require.Equal(t, int64(4), result[2].EventCount)
	})

	t.Run("min and max operators use comparisons within each hour", func(t *testing.T) {
		start := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
		end := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)

		minResult := svc.rollupToHour([]coreagg.AggregateState{
			{Operator: coreagg.OpMin, WindowStart: time.Date(2026, 2, 1, 10, 1, 0, 0, time.UTC), Value: decimal.NewFromInt(9), EventCount: 1},
			{Operator: coreagg.OpMin, WindowStart: time.Date(2026, 2, 1, 10, 30, 0, 0, time.UTC), Value: decimal.NewFromInt(4), EventCount: 1},
			{Operator: coreagg.OpMin, WindowStart: time.Date(2026, 2, 1, 11, 15, 0, 0, time.UTC), Value: decimal.NewFromInt(7), EventCount: 1},
		}, start, end)
		require.Len(t, minResult, 2)
		require.Equal(t, "4", minResult[0].Value.String())
		require.Equal(t, "7", minResult[1].Value.String())

		maxResult := svc.rollupToHour([]coreagg.AggregateState{
			{Operator: coreagg.OpMax, WindowStart: time.Date(2026, 2, 1, 10, 1, 0, 0, time.UTC), Value: decimal.NewFromInt(9), EventCount: 1},
			{Operator: coreagg.OpMax, WindowStart: time.Date(2026, 2, 1, 10, 30, 0, 0, time.UTC), Value: decimal.NewFromInt(4), EventCount: 1},
			{Operator: coreagg.OpMax, WindowStart: time.Date(2026, 2, 1, 11, 15, 0, 0, time.UTC), Value: decimal.NewFromInt(12), EventCount: 1},
		}, start, end)
		require.Len(t, maxResult, 2)
		require.Equal(t, "9", maxResult[0].Value.String())
		require.Equal(t, "12", maxResult[1].Value.String())
	})
}

func TestService_RollupToDay_Operators(t *testing.T) {
	svc := &Service{}
	start := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 3, 12, 0, 0, 0, time.UTC)

	aggregates := []coreagg.AggregateState{
		{Operator: coreagg.OpMax, Value: decimal.NewFromInt(2), EventCount: 1, WindowStart: time.Date(2026, 2, 1, 11, 0, 0, 0, time.UTC)},
		{Operator: coreagg.OpMax, Value: decimal.NewFromInt(9), EventCount: 1, WindowStart: time.Date(2026, 2, 1, 23, 0, 0, 0, time.UTC)},
		{Operator: coreagg.OpMax, Value: decimal.NewFromInt(4), EventCount: 1, WindowStart: time.Date(2026, 2, 2, 1, 0, 0, 0, time.UTC)},
		{Operator: coreagg.OpMax, Value: decimal.NewFromInt(8), EventCount: 1, WindowStart: time.Date(2026, 2, 3, 10, 0, 0, 0, time.UTC)},
	}

	result := svc.rollupToDay(aggregates, start, end)
	require.Len(t, result, 3)
	require.Equal(t, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), result[0].WindowStart)
	require.Equal(t, "9", result[0].Value.String())
	require.Equal(t, "4", result[1].Value.String())
	require.Equal(t, "8", result[2].Value.String())

	minResult := svc.rollupToDay([]coreagg.AggregateState{
		{Operator: coreagg.OpMin, Value: decimal.NewFromInt(9), EventCount: 1, WindowStart: time.Date(2026, 2, 1, 11, 0, 0, 0, time.UTC)},
		{Operator: coreagg.OpMin, Value: decimal.NewFromInt(2), EventCount: 1, WindowStart: time.Date(2026, 2, 1, 23, 0, 0, 0, time.UTC)},
		{Operator: coreagg.OpMin, Value: decimal.NewFromInt(5), EventCount: 1, WindowStart: time.Date(2026, 2, 2, 1, 0, 0, 0, time.UTC)},
	}, start, end)
	require.Len(t, minResult, 3)
	require.Equal(t, "2", minResult[0].Value.String())
	require.Equal(t, "5", minResult[1].Value.String())
}

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
