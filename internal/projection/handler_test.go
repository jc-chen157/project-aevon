package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
	aggregationmocks "github.com/aevon-lab/project-aevon/internal/mocks/aggregation"
	storagemocks "github.com/aevon-lab/project-aevon/internal/mocks/storage"
	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestService_HandleQueryAggregates_StatusMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)

	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	rules := []coreagg.AggregationRule{{
		Name:        "count_requests",
		SourceEvent: "api.request",
		Operator:    coreagg.OpCount,
		WindowSize:  time.Minute,
	}}

	tests := []struct {
		name            string
		query           string
		expectedStatus  int
		configurePreAgg func(preAggStore *aggregationmocks.PreAggregateStore)
		configureEvent  func(eventStore *storagemocks.EventStore)
	}{
		{
			name: "invalid request returns 400",
			query: fmt.Sprintf(
				"rule=count_requests&start=%s&end=%s&granularity=total",
				end.Format(time.RFC3339),
				start.Format(time.RFC3339),
			),
			expectedStatus:  http.StatusBadRequest,
			configurePreAgg: func(_ *aggregationmocks.PreAggregateStore) {},
			configureEvent:  func(_ *storagemocks.EventStore) {},
		},
		{
			name: "store error returns 500",
			query: fmt.Sprintf(
				"rule=count_requests&start=%s&end=%s&granularity=total",
				start.Format(time.RFC3339),
				end.Format(time.RFC3339),
			),
			expectedStatus: http.StatusInternalServerError,
			configurePreAgg: func(preAggStore *aggregationmocks.PreAggregateStore) {
				preAggStore.EXPECT().
					QueryRange(mock.Anything, "user-1", "count_requests", "1m", start, end).
					Return([]coreagg.AggregateState(nil), fmt.Errorf("db failure")).
					Once()
			},
			configureEvent: func(_ *storagemocks.EventStore) {},
		},
		{
			name: "raw tail error returns 500",
			query: fmt.Sprintf(
				"rule=count_requests&start=%s&end=%s&granularity=total",
				start.Format(time.RFC3339),
				end.Format(time.RFC3339),
			),
			expectedStatus: http.StatusInternalServerError,
			configurePreAgg: func(preAggStore *aggregationmocks.PreAggregateStore) {
				preAggStore.EXPECT().
					QueryRange(mock.Anything, "user-1", "count_requests", "1m", start, end).
					Return([]coreagg.AggregateState{{
						Operator:        coreagg.OpCount,
						Value:           decimal.NewFromInt(1),
						EventCount:      1,
						LastEventID:     "evt-1",
						RuleFingerprint: "fp-1",
						WindowStart:     start,
						UpdatedAt:       start.Add(time.Minute),
					}}, nil).
					Once()
				preAggStore.EXPECT().ReadCheckpoint(mock.Anything, "1m").Return(int64(42), nil).Once()
			},
			configureEvent: func(eventStore *storagemocks.EventStore) {
				eventStore.EXPECT().
					RetrieveScopedEventsAfterCursor(mock.Anything, int64(42), "user-1", "api.request", start, end, rawQueryBatchSize).
					Return(nil, fmt.Errorf("event store failure")).
					Once()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			preAggStore := aggregationmocks.NewPreAggregateStore(t)
			eventStore := storagemocks.NewEventStore(t)
			tc.configurePreAgg(preAggStore)
			tc.configureEvent(eventStore)

			svc := NewService(preAggStore, eventStore, rules)
			r := gin.New()
			svc.RegisterRoutes(r)

			url := "/v1/state/user-1?" + tc.query
			req := httptest.NewRequest(http.MethodGet, url, nil)
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			if resp.Code != tc.expectedStatus {
				t.Logf("unexpected response body: %s", resp.Body.String())
			}
			require.Equal(t, tc.expectedStatus, resp.Code)
		})
	}
}

func TestService_HandleQueryAggregates_TimeoutReturnsGatewayTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)

	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	preAggStore := aggregationmocks.NewPreAggregateStore(t)
	preAggStore.EXPECT().
		QueryRange(mock.Anything, "user-1", "count_requests", "1m", start, end).
		Return([]coreagg.AggregateState(nil), nil).
		Once()
	preAggStore.EXPECT().ReadCheckpoint(mock.Anything, "1m").Return(int64(0), nil).Once()

	eventStore := storagemocks.NewEventStore(t)
	eventStore.EXPECT().
		RetrieveScopedEventsAfterCursor(mock.Anything, int64(0), "user-1", "api.request", start, end, rawQueryBatchSize).
		RunAndReturn(func(ctx context.Context, _ int64, _ string, _ string, _ time.Time, _ time.Time, _ int) ([]*v1.Event, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).
		Once()

	svc := NewService(preAggStore, eventStore, []coreagg.AggregationRule{{
		Name:        "count_requests",
		SourceEvent: "api.request",
		Operator:    coreagg.OpCount,
		WindowSize:  time.Minute,
	}})
	svc.queryTimeout = 5 * time.Millisecond

	r := gin.New()
	svc.RegisterRoutes(r)

	url := fmt.Sprintf(
		"/v1/state/user-1?rule=count_requests&start=%s&end=%s&granularity=total",
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
	)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusGatewayTimeout, resp.Code)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &body))
	message, ok := body["message"].(string)
	require.True(t, ok)
	require.Contains(t, message, "timed out")
	details, ok := body["details"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "5ms", details["timeout"])
}

func TestService_HandleQueryAggregates_TimeoutIncludesAdviceWhenRuleMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	preAggStore := aggregationmocks.NewPreAggregateStore(t)
	eventStore := storagemocks.NewEventStore(t)
	svc := NewService(preAggStore, eventStore, nil)
	svc.queryTimeout = 0

	r := gin.New()
	svc.RegisterRoutes(r)

	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	url := fmt.Sprintf(
		"/v1/state/user-1?rule=missing_rule&start=%s&end=%s&granularity=total",
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
	)

	req := httptest.NewRequest(http.MethodGet, url, nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusGatewayTimeout, resp.Code)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &body))

	details, ok := body["details"].(map[string]interface{})
	require.True(t, ok)
	advice, ok := details["advice"].(string)
	require.True(t, ok)
	require.Contains(t, advice, "No pre-aggregation rule is configured")
	require.Contains(t, advice, "missing_rule")
}
