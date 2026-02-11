package projection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	aggstore "github.com/aevon-lab/project-aevon/internal/aggregation"
	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/aevon-lab/project-aevon/internal/core/storage"
	"github.com/shopspring/decimal"
)

const (
	defaultBucketSize     = "1m"
	rawQueryBatchSize     = 5000
	maxRawQueryIterations = 20 // Limit to prevent timeout/OOM when checkpoint is far behind
)

var (
	// ErrInvalidQuery marks request validation errors that should return HTTP 400.
	ErrInvalidQuery = errors.New("invalid aggregate query")

	bucketCandidatesByGranularity = map[string][]string{
		"total": {"1m"},
		"1m":    {"1m"},
		"1h":    {"1m"},
		"1d":    {"1m"},
	}
)

// Service implements the projection/query layer.
// It serves a hybrid read path: durable pre-aggregates + unflushed raw events.
type Service struct {
	preAggStore aggstore.PreAggregateStore
	eventStore  storage.EventStore
	rules       map[string]coreagg.AggregationRule
	nowFn       func() time.Time
}

// checkpointSnapshotReader allows projection reads to fetch checkpoint + aggregates
// from one SQL statement snapshot, eliminating interleaving races with sweeper flushes.
type checkpointSnapshotReader interface {
	QueryRangeWithCheckpoint(
		ctx context.Context,
		tenantID string,
		principalID string,
		ruleName string,
		bucketSize string,
		startTime time.Time,
		endTime time.Time,
	) ([]coreagg.AggregateState, int64, error)
}

// NewService creates a new projection service.
func NewService(
	preAggStore aggstore.PreAggregateStore,
	eventStore storage.EventStore,
	rules []coreagg.AggregationRule,
) *Service {
	ruleMap := make(map[string]coreagg.AggregationRule, len(rules))
	for _, rule := range rules {
		ruleMap[rule.Name] = rule
	}

	return &Service{
		preAggStore: preAggStore,
		eventStore:  eventStore,
		rules:       ruleMap,
		nowFn: func() time.Time {
			return time.Now().UTC()
		},
	}
}

// QueryAggregates retrieves aggregated usage data for a time range.
func (s *Service) QueryAggregates(ctx context.Context, req AggregateQueryRequest) (*AggregateQueryResponse, error) {
	req, err := s.normalizeAndValidate(req)
	if err != nil {
		return nil, err
	}

	rule, ok := s.rules[req.Rule]
	if !ok {
		return nil, invalidQueryf("unknown rule: %s", req.Rule)
	}

	preAggregates, bucketSize, checkpoint, err := s.loadPreAggregates(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("query pre-aggregates: %w", err)
	}

	merged := preAggregates
	rawAggregates, rawErr := s.loadRawEvents(ctx, req, rule, bucketSize, checkpoint)
	if rawErr != nil {
		return nil, fmt.Errorf("query raw event tail: %w", rawErr)
	}
	merged = mergeAggregateStates(merged, rawAggregates, rule.Operator)

	bucketDuration, parseErr := parseBucketSize(bucketSize)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid bucket size %q: %w", bucketSize, parseErr)
	}

	values := s.rollupForGranularity(merged, req.Granularity, bucketDuration, req.Start, req.End)

	// Compute accurate data_through based on actual data
	dataThrough := s.computeDataThrough(req.End, merged, bucketDuration)

	// Cap at current time - can't have data from the future
	dataThrough = minTime(dataThrough, s.nowFn())

	staleness := int(s.nowFn().Sub(dataThrough).Seconds())
	if staleness < 0 {
		staleness = 0
	}

	return &AggregateQueryResponse{
		TenantID:         req.TenantID,
		PrincipalID:      req.PrincipalID,
		Rule:             req.Rule,
		Operator:         rule.Operator,
		Start:            req.Start,
		End:              req.End,
		Granularity:      req.Granularity,
		DataThrough:      dataThrough,
		StalenessSeconds: staleness,
		Values:           values,
	}, nil
}

func (s *Service) normalizeAndValidate(req AggregateQueryRequest) (AggregateQueryRequest, error) {
	if req.Granularity == "" {
		req.Granularity = "total"
	}

	if req.TenantID == "" {
		return req, invalidQueryf("tenant_id is required")
	}
	if req.PrincipalID == "" {
		return req, invalidQueryf("principal_id is required")
	}
	if req.Rule == "" {
		return req, invalidQueryf("rule is required")
	}
	if !req.End.After(req.Start) {
		return req, invalidQueryf("end time must be after start time")
	}

	switch req.Granularity {
	case "total", "1m", "1h", "1d":
	default:
		return req, invalidQueryf("invalid granularity: %s (must be total, 1m, 1h, or 1d)", req.Granularity)
	}

	return req, nil
}

func (s *Service) loadPreAggregates(ctx context.Context, req AggregateQueryRequest) ([]coreagg.AggregateState, string, int64, error) {
	candidates := bucketCandidatesByGranularity[req.Granularity]
	if len(candidates) == 0 {
		candidates = []string{defaultBucketSize}
	}

	snapshotReader, hasSnapshotReader := s.preAggStore.(checkpointSnapshotReader)

	for idx, bucketSize := range candidates {
		var (
			aggregates []coreagg.AggregateState
			checkpoint int64
			err        error
		)

		if hasSnapshotReader {
			aggregates, checkpoint, err = snapshotReader.QueryRangeWithCheckpoint(
				ctx,
				req.TenantID,
				req.PrincipalID,
				req.Rule,
				bucketSize,
				req.Start,
				req.End,
			)
			if err != nil {
				return nil, "", 0, err
			}
		} else {
			aggregates, err = s.preAggStore.QueryRange(
				ctx,
				req.TenantID,
				req.PrincipalID,
				req.Rule,
				bucketSize,
				req.Start,
				req.End,
			)
			if err != nil {
				return nil, "", 0, err
			}

			checkpoint, err = s.preAggStore.ReadCheckpoint(ctx, bucketSize)
			if err != nil {
				return nil, "", 0, fmt.Errorf("read checkpoint: %w", err)
			}
		}

		if len(aggregates) > 0 || idx == len(candidates)-1 {
			return aggregates, bucketSize, checkpoint, nil
		}
	}

	return nil, defaultBucketSize, 0, nil
}

func (s *Service) loadRawEvents(
	ctx context.Context,
	req AggregateQueryRequest,
	rule coreagg.AggregationRule,
	bucketSize string,
	checkpoint int64,
) ([]coreagg.AggregateState, error) {
	bucketDuration, err := parseBucketSize(bucketSize)
	if err != nil {
		return nil, err
	}

	reducer, ok := coreagg.Operators[rule.Operator]
	if !ok {
		return nil, fmt.Errorf("unknown rule operator: %s", rule.Operator)
	}

	buckets := make(map[time.Time]coreagg.AggregateState)
	err = s.scanScopedRawEvents(ctx, checkpoint, req, rule.SourceEvent, func(events []*v1.Event) {
		s.foldRawEventsIntoBuckets(events, buckets, rule, reducer, bucketDuration)
	})
	if err != nil {
		return nil, err
	}

	results := make([]coreagg.AggregateState, 0, len(buckets))
	for _, state := range buckets {
		results = append(results, state)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].WindowStart.Before(results[j].WindowStart)
	})

	return results, nil
}

func (s *Service) scanScopedRawEvents(
	ctx context.Context,
	cursor int64,
	req AggregateQueryRequest,
	eventType string,
	consume func(events []*v1.Event),
) error {
	iterations := 0
	totalEvents := 0

	for {
		// Safety limit: prevent unbounded scanning if checkpoint is far behind
		if iterations >= maxRawQueryIterations {
			slog.Warn("Raw event tail scan reached maximum iteration limit",
				"tenant", req.TenantID,
				"principal", req.PrincipalID,
				"iterations", iterations,
				"events_scanned", totalEvents,
				"max_iterations", maxRawQueryIterations,
			)
			return fmt.Errorf("raw event scan exceeded maximum iterations (%d batches, %d events total) - aggregation may be too far behind",
				maxRawQueryIterations, totalEvents)
		}

		events, queryErr := s.eventStore.RetrieveScopedEventsAfterCursor(
			ctx,
			cursor,
			req.TenantID,
			req.PrincipalID,
			eventType,
			req.Start,
			req.End,
			rawQueryBatchSize,
		)
		if queryErr != nil {
			return queryErr
		}
		if len(events) == 0 {
			return nil
		}

		consume(events)
		totalEvents += len(events)
		iterations++

		cursor = events[len(events)-1].IngestSeq
		if len(events) < rawQueryBatchSize {
			return nil
		}
	}
}

func (s *Service) foldRawEventsIntoBuckets(
	events []*v1.Event,
	buckets map[time.Time]coreagg.AggregateState,
	rule coreagg.AggregationRule,
	reducer coreagg.Aggregator,
	bucketDuration time.Duration,
) {
	for _, evt := range events {
		windowStart := coreagg.BucketFor(evt.OccurredAt, bucketDuration)
		fieldValue := coreagg.ExtractDecimal(evt.Data, rule.Field)

		state, exists := buckets[windowStart]
		if !exists {
			buckets[windowStart] = coreagg.AggregateState{
				Operator:        rule.Operator,
				Value:           reducer.Initial(fieldValue),
				EventCount:      1,
				LastEventID:     evt.ID,
				RuleFingerprint: rule.Fingerprint,
				WindowStart:     windowStart,
				UpdatedAt:       resolveEventUpdatedAt(evt, s.nowFn()),
			}
			continue
		}

		state.Value = reducer.Apply(state.Value, fieldValue)
		state.EventCount++
		state.LastEventID = evt.ID
		state.UpdatedAt = maxTime(state.UpdatedAt, resolveEventUpdatedAt(evt, s.nowFn()))
		buckets[windowStart] = state
	}
}

func (s *Service) rollupForGranularity(
	aggregates []coreagg.AggregateState,
	granularity string,
	bucketDuration time.Duration,
	start, end time.Time,
) []AggregateValue {
	switch granularity {
	case "total":
		return s.rollupTotal(aggregates, start, end)
	case "1m":
		return s.convertToValues(aggregates, bucketDuration)
	case "1h":
		if bucketDuration == time.Hour {
			return s.convertToValues(aggregates, bucketDuration)
		}
		return s.rollupToHour(aggregates, start, end)
	case "1d":
		if bucketDuration == 24*time.Hour {
			return s.convertToValues(aggregates, bucketDuration)
		}
		return s.rollupToDay(aggregates, start, end)
	default:
		return s.rollupTotal(aggregates, start, end)
	}
}

func (s *Service) computeDataThrough(end time.Time, aggregates []coreagg.AggregateState, bucketDuration time.Duration) time.Time {
	if len(aggregates) == 0 {
		// Empty result still means query is complete up to requested end.
		return end
	}

	var dataThrough time.Time
	for _, agg := range aggregates {
		windowEnd := agg.WindowStart.Add(bucketDuration)
		if windowEnd.After(dataThrough) {
			dataThrough = windowEnd
		}
	}
	if dataThrough.After(end) {
		return end
	}
	return dataThrough
}

func mergeAggregateStates(
	base []coreagg.AggregateState,
	tail []coreagg.AggregateState,
	operator string,
) []coreagg.AggregateState {
	merged := make(map[time.Time]coreagg.AggregateState, len(base)+len(tail))
	for _, state := range base {
		merged[state.WindowStart] = state
	}

	for _, incoming := range tail {
		current, exists := merged[incoming.WindowStart]
		if !exists {
			merged[incoming.WindowStart] = incoming
			continue
		}

		current.Value = mergeAggregateValue(operator, current.Value, incoming.Value)
		current.EventCount += incoming.EventCount
		if incoming.LastEventID != "" {
			current.LastEventID = incoming.LastEventID
		}
		if incoming.RuleFingerprint != "" {
			current.RuleFingerprint = incoming.RuleFingerprint
		}
		current.UpdatedAt = maxTime(current.UpdatedAt, incoming.UpdatedAt)
		merged[incoming.WindowStart] = current
	}

	results := make([]coreagg.AggregateState, 0, len(merged))
	for _, state := range merged {
		results = append(results, state)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].WindowStart.Before(results[j].WindowStart)
	})

	return results
}

func mergeAggregateValue(operator string, current, incoming decimal.Decimal) decimal.Decimal {
	switch operator {
	case coreagg.OpCount, coreagg.OpSum:
		return current.Add(incoming)
	case coreagg.OpMin:
		if incoming.LessThan(current) {
			return incoming
		}
		return current
	case coreagg.OpMax:
		if incoming.GreaterThan(current) {
			return incoming
		}
		return current
	default:
		return incoming
	}
}

func parseBucketSize(label string) (time.Duration, error) {
	if label == "" {
		return time.Minute, nil
	}

	if strings.HasSuffix(label, "m") {
		n, err := strconv.Atoi(strings.TrimSuffix(label, "m"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid minute bucket %q", label)
		}
		return time.Duration(n) * time.Minute, nil
	}

	if strings.HasSuffix(label, "h") {
		n, err := strconv.Atoi(strings.TrimSuffix(label, "h"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid hour bucket %q", label)
		}
		return time.Duration(n) * time.Hour, nil
	}

	if strings.HasSuffix(label, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(label, "d"))
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid day bucket %q", label)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("unsupported bucket size: %q", label)
}

func resolveEventUpdatedAt(evt *v1.Event, fallback time.Time) time.Time {
	if evt != nil && !evt.IngestedAt.IsZero() {
		return evt.IngestedAt.UTC()
	}
	return fallback.UTC()
}

func invalidQueryf(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrInvalidQuery, fmt.Sprintf(format, args...))
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
