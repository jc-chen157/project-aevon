package aggregation

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	"github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/aevon-lab/project-aevon/internal/core/partition"
	"github.com/aevon-lab/project-aevon/internal/core/storage"
	"github.com/shopspring/decimal"
)

const (
	defaultBatchSize   = 50000
	defaultWorkerCount = 10
)

// BatchJobParameter controls throughput and aggregation behavior for a batch run.
type BatchJobParameter struct {
	BatchSize   int
	WorkerCount int
	BucketSize  time.Duration
	BucketLabel string
}

// DefaultBatchJobOptions returns safe defaults for cron-based processing.
func DefaultBatchJobOptions() BatchJobParameter {
	return BatchJobParameter{
		BatchSize:   defaultBatchSize,
		WorkerCount: defaultWorkerCount,
		BucketSize:  time.Minute,
		BucketLabel: "1m",
	}
}

func (o BatchJobParameter) normalized() BatchJobParameter {
	n := o
	if n.BatchSize <= 0 {
		n.BatchSize = defaultBatchSize
	}
	if n.WorkerCount <= 0 {
		n.WorkerCount = defaultWorkerCount
	}
	if n.BucketSize <= 0 {
		n.BucketSize = time.Minute
	}
	if n.BucketLabel == "" {
		n.BucketLabel = windowSizeLabel(n.BucketSize)
	}
	return n
}

// RunBatchAggregation processes events since last checkpoint and updates aggregates.
// Uses default options: 50K batch size, 10 workers, 1-minute buckets.
func RunBatchAggregation(
	ctx context.Context,
	eventStore storage.EventStore,
	preAggStore PreAggregateStore,
	rules []aggregation.AggregationRule,
) error {
	return RunBatchAggregationWithOptions(ctx, eventStore, preAggStore, rules, DefaultBatchJobOptions())
}

// RunBatchAggregationWithOptions processes events since last checkpoint with configurable
// batch size, worker count and bucket duration.
func RunBatchAggregationWithOptions(
	ctx context.Context,
	eventStore storage.EventStore,
	preAggStore PreAggregateStore,
	rules []aggregation.AggregationRule,
	jobParameter BatchJobParameter,
) error {
	jobParameter = jobParameter.normalized()

	cursor, err := preAggStore.ReadCheckpoint(ctx, jobParameter.BucketLabel)
	if err != nil {
		return fmt.Errorf("read checkpoint: %w", err)
	}

	slog.Info("[BatchJob] Starting batch aggregation",
		"cursor", cursor,
		"bucket_size", jobParameter.BucketLabel,
		"batch_size", jobParameter.BatchSize,
		"workers", jobParameter.WorkerCount,
	)

	events, err := eventStore.RetrieveEventsAfterCursor(ctx, cursor, jobParameter.BatchSize)
	if err != nil {
		return fmt.Errorf("query events: %w", err)
	}

	if len(events) == 0 {
		slog.Debug("[BatchJob] No new events to process", "bucket_size", jobParameter.BucketLabel)
		return nil
	}

	slog.Info("[BatchJob] Processing events",
		"count", len(events),
		"from_cursor", cursor,
		"bucket_size", jobParameter.BucketLabel,
	)

	ruleMap := toCompiledRuleMap(rules)
	aggregates := buildPreAggregatesConcurrently(events, ruleMap, jobParameter)

	slog.Info("[BatchJob] Computed aggregates",
		"aggregate_count", len(aggregates),
		"bucket_size", jobParameter.BucketLabel,
	)

	newCursor := events[len(events)-1].IngestSeq
	if err := preAggStore.Flush(ctx, aggregates, newCursor, jobParameter.BucketLabel); err != nil {
		return fmt.Errorf("flush aggregates: %w", err)
	}

	slog.Info("[BatchJob] Batch complete",
		"events_processed", len(events),
		"aggregates_computed", len(aggregates),
		"cursor_advanced", fmt.Sprintf("%d -> %d", cursor, newCursor),
		"bucket_size", jobParameter.BucketLabel,
	)

	return nil
}

// RunBatchAggregationWithOptionsReturningCount is like RunBatchAggregationWithOptions
// but returns the number of events processed. This is used by the scheduler to
// determine if there's more backlog to drain.
func RunBatchAggregationWithOptionsReturningCount(
	ctx context.Context,
	eventStore storage.EventStore,
	preAggregateStore PreAggregateStore,
	rules []aggregation.AggregationRule,
	opts BatchJobParameter,
) (int, error) {
	opts = opts.normalized()

	cursor, err := preAggregateStore.ReadCheckpoint(ctx, opts.BucketLabel)
	if err != nil {
		return 0, fmt.Errorf("read checkpoint: %w", err)
	}

	events, err := eventStore.RetrieveEventsAfterCursor(ctx, cursor, opts.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("query events: %w", err)
	}

	if len(events) == 0 {
		return 0, nil
	}

	ruleMap := toCompiledRuleMap(rules)
	aggregates := buildPreAggregatesConcurrently(events, ruleMap, opts)

	newCursor := events[len(events)-1].IngestSeq
	if err := preAggregateStore.Flush(ctx, aggregates, newCursor, opts.BucketLabel); err != nil {
		return 0, fmt.Errorf("flush aggregates: %w", err)
	}

	slog.Info("[BatchJob] Batch complete",
		"events_processed", len(events),
		"aggregates_computed", len(aggregates),
		"cursor_advanced", fmt.Sprintf("%d -> %d", cursor, newCursor),
		"bucket_size", opts.BucketLabel,
	)

	return len(events), nil
}

type eventGroupKey struct {
	PrincipalID string
}

type compiledRule struct {
	rule aggregation.AggregationRule
	agg  aggregation.Aggregator
}

func toCompiledRuleMap(rules []aggregation.AggregationRule) map[string][]compiledRule {
	ruleMap := make(map[string][]compiledRule)
	for _, r := range rules {
		agg, ok := aggregation.Operators[r.Operator]
		if !ok {
			slog.Warn("[BatchJob] Skip rule with unknown operator", "rule", r.Name, "operator", r.Operator)
			continue
		}
		ruleMap[r.SourceEvent] = append(ruleMap[r.SourceEvent], compiledRule{rule: r, agg: agg})
	}
	return ruleMap
}

func buildPreAggregatesConcurrently(
	events []*v1.Event,
	ruleMap map[string][]compiledRule,
	jobParameter BatchJobParameter,
) map[aggregation.AggregateKey]aggregation.AggregateState {
	groups := make(map[eventGroupKey][]*v1.Event)
	for _, evt := range events {
		key := eventGroupKey{PrincipalID: evt.PrincipalID}
		groups[key] = append(groups[key], evt)
	}

	jobs := make(chan []*v1.Event, len(groups))
	results := make(chan map[aggregation.AggregateKey]aggregation.AggregateState, minInt(jobParameter.WorkerCount, len(groups)))

	workerCount := minInt(jobParameter.WorkerCount, len(groups))
	if workerCount <= 0 {
		return map[aggregation.AggregateKey]aggregation.AggregateState{}
	}

	now := time.Now().UTC()
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			local := make(map[aggregation.AggregateKey]aggregation.AggregateState)
			for groupEvents := range jobs {
				mergeGroupAggregates(local, groupEvents, ruleMap, jobParameter, now)
			}
			results <- local
		}()
	}

	for _, groupedEvents := range groups {
		jobs <- groupedEvents
	}
	close(jobs)

	wg.Wait()
	close(results)

	merged := make(map[aggregation.AggregateKey]aggregation.AggregateState)
	for local := range results {
		for key, state := range local {
			if existing, ok := merged[key]; ok {
				existing.Value = mergeValueByOperator(existing.Operator, existing.Value, state.Value)
				existing.EventCount += state.EventCount
				existing.LastEventID = state.LastEventID
				existing.RuleFingerprint = state.RuleFingerprint
				existing.UpdatedAt = maxTime(existing.UpdatedAt, state.UpdatedAt)
				merged[key] = existing
				continue
			}
			merged[key] = state
		}
	}

	return merged
}

func mergeGroupAggregates(
	target map[aggregation.AggregateKey]aggregation.AggregateState,
	events []*v1.Event,
	ruleCache map[string][]compiledRule,
	opts BatchJobParameter,
	now time.Time,
) {
	for _, evt := range events {
		rulesForEvent, ok := ruleCache[evt.Type]
		if !ok {
			continue
		}

		for _, cr := range rulesForEvent {
			windowStart := aggregation.BucketFor(evt.OccurredAt, opts.BucketSize)
			key := aggregation.AggregateKey{
				PartitionID: partition.For(evt.PrincipalID),
				PrincipalID: evt.PrincipalID,
				RuleName:    cr.rule.Name,
				BucketSize:  opts.BucketLabel,
				WindowStart: windowStart,
			}

			incoming := aggregation.ExtractDecimal(evt.Data, cr.rule.Field)
			state, exists := target[key]
			if !exists {
				target[key] = aggregation.AggregateState{
					Operator:        cr.rule.Operator,
					Value:           cr.agg.Initial(incoming),
					EventCount:      1,
					LastEventID:     evt.ID,
					RuleFingerprint: cr.rule.Fingerprint,
					UpdatedAt:       now,
				}
				continue
			}

			state.Value = cr.agg.Apply(state.Value, incoming)
			state.EventCount++
			state.LastEventID = evt.ID
			state.UpdatedAt = now
			target[key] = state
		}
	}
}

func mergeValueByOperator(operator string, current, incoming decimal.Decimal) decimal.Decimal {
	switch operator {
	case aggregation.OpCount, aggregation.OpSum:
		return current.Add(incoming)
	case aggregation.OpMin:
		if incoming.LessThan(current) {
			return incoming
		}
		return current
	case aggregation.OpMax:
		if incoming.GreaterThan(current) {
			return incoming
		}
		return current
	default:
		return incoming
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func windowSizeLabel(d time.Duration) string {
	if d%(24*time.Hour) == 0 {
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	}
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", d/time.Hour)
	}
	if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	if d%time.Second == 0 {
		return fmt.Sprintf("%ds", d/time.Second)
	}
	return d.String()
}
