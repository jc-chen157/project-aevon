package projection

import (
	"context"
	"testing"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
	aggregationmocks "github.com/aevon-lab/project-aevon/internal/mocks/aggregation"
	storagemocks "github.com/aevon-lab/project-aevon/internal/mocks/storage"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestService_QueryAggregates_Validation(t *testing.T) {
	rules := []coreagg.AggregationRule{{
		Name:        "count_requests",
		SourceEvent: "api.request",
		Operator:    coreagg.OpCount,
		WindowSize:  time.Minute,
	}}

	preAggStore := aggregationmocks.NewPreAggregateStore(t)
	eventStore := storagemocks.NewEventStore(t)
	svc := NewService(preAggStore, eventStore, rules)
	now := time.Date(2026, 2, 7, 11, 0, 0, 0, time.UTC)
	svc.nowFn = func() time.Time { return now }

	tests := []struct {
		name string
		req  AggregateQueryRequest
	}{
		{
			name: "end before start",
			req: AggregateQueryRequest{
				TenantID:    "tenant-1",
				PrincipalID: "user-1",
				Rule:        "count_requests",
				Start:       now,
				End:         now.Add(-time.Minute),
			},
		},
		{
			name: "invalid granularity",
			req: AggregateQueryRequest{
				TenantID:    "tenant-1",
				PrincipalID: "user-1",
				Rule:        "count_requests",
				Start:       now.Add(-time.Hour),
				End:         now,
				Granularity: "5m",
			},
		},
		{
			name: "unknown rule",
			req: AggregateQueryRequest{
				TenantID:    "tenant-1",
				PrincipalID: "user-1",
				Rule:        "missing_rule",
				Start:       now.Add(-time.Hour),
				End:         now,
				Granularity: "total",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.QueryAggregates(context.Background(), tc.req)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidQuery)
		})
	}
}

func TestService_QueryAggregates_EmptyResultSetsDataThroughToEnd(t *testing.T) {
	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	now := end.Add(30 * time.Minute)

	preAggStore := aggregationmocks.NewPreAggregateStore(t)
	preAggStore.EXPECT().
		QueryRange(mock.Anything, "tenant-1", "user-1", "count_requests", "1m", start, end).
		Return([]coreagg.AggregateState(nil), nil).
		Once()
	preAggStore.EXPECT().ReadCheckpoint(mock.Anything, "1m").Return(int64(0), nil).Once()

	eventStore := storagemocks.NewEventStore(t)
	eventStore.EXPECT().
		RetrieveScopedEventsAfterCursor(mock.Anything, int64(0), "tenant-1", "user-1", "api.request", start, end, rawQueryBatchSize).
		Return([]*v1.Event{}, nil).
		Once()

	rules := []coreagg.AggregationRule{{
		Name:        "count_requests",
		SourceEvent: "api.request",
		Operator:    coreagg.OpCount,
		WindowSize:  time.Minute,
		Fingerprint: "fp-1",
	}}

	svc := NewService(preAggStore, eventStore, rules)
	svc.nowFn = func() time.Time { return now }

	resp, err := svc.QueryAggregates(context.Background(), AggregateQueryRequest{
		TenantID:    "tenant-1",
		PrincipalID: "user-1",
		Rule:        "count_requests",
		Start:       start,
		End:         end,
		Granularity: "total",
	})
	require.NoError(t, err)
	require.Equal(t, end, resp.DataThrough)
	require.Equal(t, int(now.Sub(end).Seconds()), resp.StalenessSeconds)
	require.Len(t, resp.Values, 1)
	require.Equal(t, "0", resp.Values[0].Value.String())
}

func TestService_QueryAggregates_HybridMergesRawTail(t *testing.T) {
	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
	now := end.Add(10 * time.Minute)

	preAggStore := aggregationmocks.NewPreAggregateStore(t)
	preAggStore.EXPECT().
		QueryRange(mock.Anything, "tenant-1", "user-1", "count_requests", "1m", start, end).
		Return([]coreagg.AggregateState{
			{
				Operator:        coreagg.OpCount,
				Value:           decimal.NewFromInt(10),
				EventCount:      10,
				LastEventID:     "evt-10",
				RuleFingerprint: "fp-1",
				WindowStart:     start,
				UpdatedAt:       start.Add(time.Minute),
			},
		}, nil).
		Once()
	preAggStore.EXPECT().ReadCheckpoint(mock.Anything, "1m").Return(int64(100), nil).Once()

	eventStore := storagemocks.NewEventStore(t)
	eventStore.EXPECT().
		RetrieveScopedEventsAfterCursor(mock.Anything, int64(100), "tenant-1", "user-1", "api.request", start, end, rawQueryBatchSize).
		Return([]*v1.Event{
			{
				ID:          "evt-11",
				TenantID:    "tenant-1",
				PrincipalID: "user-1",
				Type:        "api.request",
				OccurredAt:  start.Add(30 * time.Second),
				IngestedAt:  start.Add(11 * time.Minute),
				IngestSeq:   101,
				Data:        map[string]interface{}{},
			},
			{
				ID:          "evt-12",
				TenantID:    "tenant-1",
				PrincipalID: "user-1",
				Type:        "api.request",
				OccurredAt:  start.Add(40 * time.Second),
				IngestedAt:  start.Add(12 * time.Minute),
				IngestSeq:   102,
				Data:        map[string]interface{}{},
			},
		}, nil).
		Once()

	rules := []coreagg.AggregationRule{{
		Name:        "count_requests",
		SourceEvent: "api.request",
		Operator:    coreagg.OpCount,
		WindowSize:  time.Minute,
		Fingerprint: "fp-1",
	}}

	svc := NewService(preAggStore, eventStore, rules)
	svc.nowFn = func() time.Time { return now }

	resp, err := svc.QueryAggregates(context.Background(), AggregateQueryRequest{
		TenantID:    "tenant-1",
		PrincipalID: "user-1",
		Rule:        "count_requests",
		Start:       start,
		End:         end,
		Granularity: "total",
	})
	require.NoError(t, err)
	require.Len(t, resp.Values, 1)
	require.Equal(t, "12", resp.Values[0].Value.String())
	require.Equal(t, int64(12), resp.Values[0].EventCount)
	// DataThrough should reflect the actual data (10:01), not the query end time (10:10)
	// The pre-aggregate bucket is [10:00, 10:01), and raw events are at 10:00:30, 10:00:40
	require.Equal(t, start.Add(1*time.Minute), resp.DataThrough)
}

func TestService_QueryAggregates_UsesFixed1mBucket(t *testing.T) {
	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	calledBuckets := make([]string, 0, 1)
	preAggStore := aggregationmocks.NewPreAggregateStore(t)
	preAggStore.EXPECT().
		QueryRange(mock.Anything, "tenant-1", "user-1", "sum_bytes", "1m", start, end).
		Run(func(ctx context.Context, tenantID string, principalID string, ruleName string, bucketSize string, startTime time.Time, endTime time.Time) {
			calledBuckets = append(calledBuckets, bucketSize)
		}).
		Return([]coreagg.AggregateState{{
			Operator:        coreagg.OpSum,
			Value:           decimal.NewFromInt(8),
			EventCount:      2,
			LastEventID:     "evt-2",
			RuleFingerprint: "fp-1",
			WindowStart:     start.Add(10 * time.Minute),
			UpdatedAt:       start.Add(20 * time.Minute),
		}}, nil).
		Once()
	preAggStore.EXPECT().ReadCheckpoint(mock.Anything, "1m").Return(int64(0), nil).Once()

	eventStore := storagemocks.NewEventStore(t)
	eventStore.EXPECT().
		RetrieveScopedEventsAfterCursor(mock.Anything, int64(0), "tenant-1", "user-1", "api.request", start, end, rawQueryBatchSize).
		Return([]*v1.Event{}, nil).
		Once()

	rules := []coreagg.AggregationRule{{
		Name:        "sum_bytes",
		SourceEvent: "api.request",
		Operator:    coreagg.OpSum,
		Field:       "bytes",
		WindowSize:  time.Minute,
		Fingerprint: "fp-1",
	}}

	svc := NewService(preAggStore, eventStore, rules)
	resp, err := svc.QueryAggregates(context.Background(), AggregateQueryRequest{
		TenantID:    "tenant-1",
		PrincipalID: "user-1",
		Rule:        "sum_bytes",
		Start:       start,
		End:         end,
		Granularity: "1h",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"1m"}, calledBuckets)
	require.Len(t, resp.Values, 1)
	require.Equal(t, "8", resp.Values[0].Value.String())
	require.Equal(t, int64(2), resp.Values[0].EventCount)
}
