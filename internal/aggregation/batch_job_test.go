package aggregation

import (
	"context"
	"testing"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	"github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockEventStore for testing
type mockEventStore struct {
	events []*v1.Event
}

func (m *mockEventStore) SaveEvent(ctx context.Context, event *v1.Event) error {
	m.events = append(m.events, event)
	return nil
}

func (m *mockEventStore) RetrieveEventsAfter(ctx context.Context, afterTime time.Time, limit int) ([]*v1.Event, error) {
	return nil, nil // Not used in batch job
}

func (m *mockEventStore) RetrieveEventsByPrincipalAndIngestedRange(
	ctx context.Context,
	principalID string,
	startIngestedAt time.Time,
	endIngestedAt time.Time,
	limit int,
) ([]*v1.Event, error) {
	return nil, nil
}

func (m *mockEventStore) RetrieveEventsAfterCursor(ctx context.Context, cursor int64, limit int) ([]*v1.Event, error) {
	var result []*v1.Event
	for _, evt := range m.events {
		if evt.IngestSeq > cursor {
			result = append(result, evt)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *mockEventStore) RetrieveScopedEventsAfterCursor(
	ctx context.Context,
	cursor int64,
	principalID string,
	eventType string,
	startOccurredAt time.Time,
	endOccurredAt time.Time,
	limit int,
) ([]*v1.Event, error) {
	return nil, nil
}

// mockPreAggStore for testing
type mockPreAggStore struct {
	checkpoints map[string]int64
	aggregates  map[aggregation.AggregateKey]aggregation.AggregateState
}

func (m *mockPreAggStore) ReadCheckpoint(ctx context.Context, bucketSize string) (int64, error) {
	if bucketSize == "" {
		bucketSize = "1m"
	}
	return m.checkpoints[bucketSize], nil
}

func (m *mockPreAggStore) Flush(ctx context.Context, aggregates map[aggregation.AggregateKey]aggregation.AggregateState, cursor int64, bucketSize string) error {
	// Merge aggregates (simulate SQL ON CONFLICT DO UPDATE)
	for k, v := range aggregates {
		m.aggregates[k] = v
	}
	if bucketSize == "" {
		bucketSize = "1m"
	}
	m.checkpoints[bucketSize] = cursor
	return nil
}

func (m *mockPreAggStore) LoadAggregates(ctx context.Context) (map[aggregation.AggregateKey]aggregation.AggregateState, error) {
	return m.aggregates, nil
}

func (m *mockPreAggStore) QueryRange(ctx context.Context, principalID string, ruleName string, bucketSize string, startTime time.Time, endTime time.Time) ([]aggregation.AggregateState, error) {
	var results []aggregation.AggregateState
	for k, v := range m.aggregates {
		if k.PrincipalID == principalID && k.RuleName == ruleName {
			if bucketSize != "" && k.BucketSize != "" && k.BucketSize != bucketSize {
				continue
			}
			if k.WindowStart.Equal(startTime) || (k.WindowStart.After(startTime) && k.WindowStart.Before(endTime)) {
				results = append(results, v)
			}
		}
	}
	return results, nil
}

func TestBatchJob_NoEvents(t *testing.T) {
	ctx := context.Background()
	eventStore := &mockEventStore{}
	preAggStore := &mockPreAggStore{
		checkpoints: map[string]int64{"1m": 0},
		aggregates:  make(map[aggregation.AggregateKey]aggregation.AggregateState),
	}
	rules := []aggregation.AggregationRule{}

	err := RunBatchAggregation(ctx, eventStore, preAggStore, rules)
	require.NoError(t, err)

	// Checkpoint should remain at 0
	assert.Equal(t, int64(0), preAggStore.checkpoints["1m"])
	assert.Empty(t, preAggStore.aggregates)
}

func TestBatchJob_CountAggregation(t *testing.T) {
	ctx := context.Background()

	// Create events
	now := time.Now().UTC().Truncate(time.Minute)
	events := []*v1.Event{
		{
			ID:          "evt-1",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   1,
			Data:        map[string]interface{}{},
		},
		{
			ID:          "evt-2",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   2,
			Data:        map[string]interface{}{},
		},
		{
			ID:          "evt-3",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   3,
			Data:        map[string]interface{}{},
		},
	}

	eventStore := &mockEventStore{events: events}
	preAggStore := &mockPreAggStore{
		checkpoints: map[string]int64{"1m": 0},
		aggregates:  make(map[aggregation.AggregateKey]aggregation.AggregateState),
	}

	rules := []aggregation.AggregationRule{
		{
			Name:        "count_requests",
			SourceEvent: "api.request",
			Operator:    aggregation.OpCount,
			WindowSize:  time.Minute,
			Fingerprint: "fp1",
		},
	}

	err := RunBatchAggregation(ctx, eventStore, preAggStore, rules)
	require.NoError(t, err)

	// Checkpoint should advance to 3
	assert.Equal(t, int64(3), preAggStore.checkpoints["1m"])

	// Should have 1 aggregate (all 3 events in same window)
	assert.Len(t, preAggStore.aggregates, 1)

	// Verify aggregate value
	var foundKey aggregation.AggregateKey
	for k := range preAggStore.aggregates {
		if k.RuleName == "count_requests" {
			foundKey = k
			break
		}
	}

	state := preAggStore.aggregates[foundKey]
	assert.Equal(t, "count", state.Operator)
	assert.Equal(t, "3", state.Value.String())
	assert.Equal(t, int64(3), state.EventCount)
	assert.Equal(t, "evt-3", state.LastEventID)
}

func TestBatchJob_SumAggregation(t *testing.T) {
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Minute)
	events := []*v1.Event{
		{
			ID:          "evt-1",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   1,
			Data:        map[string]interface{}{"bytes": 100.0},
		},
		{
			ID:          "evt-2",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   2,
			Data:        map[string]interface{}{"bytes": 250.0},
		},
	}

	eventStore := &mockEventStore{events: events}
	preAggStore := &mockPreAggStore{
		checkpoints: map[string]int64{"1m": 0},
		aggregates:  make(map[aggregation.AggregateKey]aggregation.AggregateState),
	}

	rules := []aggregation.AggregationRule{
		{
			Name:        "sum_bytes",
			SourceEvent: "api.request",
			Operator:    aggregation.OpSum,
			Field:       "bytes",
			WindowSize:  time.Minute,
			Fingerprint: "fp1",
		},
	}

	err := RunBatchAggregation(ctx, eventStore, preAggStore, rules)
	require.NoError(t, err)

	assert.Len(t, preAggStore.aggregates, 1)

	var foundKey aggregation.AggregateKey
	for k := range preAggStore.aggregates {
		foundKey = k
		break
	}

	state := preAggStore.aggregates[foundKey]
	assert.Equal(t, "sum", state.Operator)
	assert.Equal(t, "350", state.Value.String())
	assert.Equal(t, int64(2), state.EventCount)
}

func TestBatchJob_MultipleWindows(t *testing.T) {
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Minute)
	events := []*v1.Event{
		{
			ID:          "evt-1",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   1,
			Data:        map[string]interface{}{},
		},
		{
			ID:          "evt-2",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now.Add(2 * time.Minute), // Different window
			IngestSeq:   2,
			Data:        map[string]interface{}{},
		},
	}

	eventStore := &mockEventStore{events: events}
	preAggStore := &mockPreAggStore{
		checkpoints: map[string]int64{"1m": 0},
		aggregates:  make(map[aggregation.AggregateKey]aggregation.AggregateState),
	}

	rules := []aggregation.AggregationRule{
		{
			Name:        "count_requests",
			SourceEvent: "api.request",
			Operator:    aggregation.OpCount,
			WindowSize:  time.Minute,
			Fingerprint: "fp1",
		},
	}

	err := RunBatchAggregation(ctx, eventStore, preAggStore, rules)
	require.NoError(t, err)

	// Should have 2 aggregates (different windows)
	assert.Len(t, preAggStore.aggregates, 2)

	// Each aggregate should have count = 1
	for _, state := range preAggStore.aggregates {
		assert.Equal(t, "1", state.Value.String())
		assert.Equal(t, int64(1), state.EventCount)
	}
}

func TestBatchJob_Idempotency(t *testing.T) {
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Minute)
	events := []*v1.Event{
		{
			ID:          "evt-1",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   1,
			Data:        map[string]interface{}{},
		},
	}

	eventStore := &mockEventStore{events: events}
	preAggStore := &mockPreAggStore{
		checkpoints: map[string]int64{"1m": 0},
		aggregates:  make(map[aggregation.AggregateKey]aggregation.AggregateState),
	}

	rules := []aggregation.AggregationRule{
		{
			Name:        "count_requests",
			SourceEvent: "api.request",
			Operator:    aggregation.OpCount,
			WindowSize:  time.Minute,
			Fingerprint: "fp1",
		},
	}

	// Run batch twice
	err := RunBatchAggregation(ctx, eventStore, preAggStore, rules)
	require.NoError(t, err)

	assert.Equal(t, int64(1), preAggStore.checkpoints["1m"])
	firstValue := decimal.NewFromInt(0)
	for _, state := range preAggStore.aggregates {
		firstValue = state.Value
	}

	// Reset checkpoint to 0 (simulate crash before checkpoint write)
	preAggStore.checkpoints["1m"] = 0

	// Run batch again
	err = RunBatchAggregation(ctx, eventStore, preAggStore, rules)
	require.NoError(t, err)

	// Aggregate should still be 1 (idempotent upsert overwrites with same value)
	for _, state := range preAggStore.aggregates {
		assert.Equal(t, firstValue.String(), state.Value.String())
	}
}

func TestBatchJob_BucketScopedCheckpoint(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)

	events := []*v1.Event{
		{
			ID:          "evt-1",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now,
			IngestSeq:   1,
			Data:        map[string]interface{}{},
		},
		{
			ID:          "evt-2",
			PrincipalID: "user:alice",
			Type:        "api.request",
			OccurredAt:  now.Add(30 * time.Second),
			IngestSeq:   2,
			Data:        map[string]interface{}{},
		},
	}

	eventStore := &mockEventStore{events: events}
	preAggStore := &mockPreAggStore{
		checkpoints: map[string]int64{},
		aggregates:  make(map[aggregation.AggregateKey]aggregation.AggregateState),
	}
	rules := []aggregation.AggregationRule{
		{
			Name:        "count_requests",
			SourceEvent: "api.request",
			Operator:    aggregation.OpCount,
			WindowSize:  time.Minute,
			Fingerprint: "fp1",
		},
	}

	err := RunBatchAggregationWithOptions(ctx, eventStore, preAggStore, rules, BatchJobOptions{
		BatchSize:   50000,
		WorkerCount: 10,
		BucketSize:  time.Minute,
		BucketLabel: "1m",
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), preAggStore.checkpoints["1m"])

	err = RunBatchAggregationWithOptions(ctx, eventStore, preAggStore, rules, BatchJobOptions{
		BatchSize:   50000,
		WorkerCount: 10,
		BucketSize:  10 * time.Minute,
		BucketLabel: "10m",
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), preAggStore.checkpoints["10m"])

	var has1m, has10m bool
	for key := range preAggStore.aggregates {
		if key.BucketSize == "1m" {
			has1m = true
		}
		if key.BucketSize == "10m" {
			has10m = true
		}
	}
	require.True(t, has1m)
	require.True(t, has10m)
}
