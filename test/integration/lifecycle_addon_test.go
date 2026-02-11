//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	"github.com/stretchr/testify/require"
)

func TestCoreAPI_E2ELifecycle_AddOn(t *testing.T) {
	h := startHarnessWithoutScheduler(t)
	defer h.close(t)

	require.NoError(t, resetDatabase(t, h.db))

	// Projection QueryRange currently reads partition_id=0; use a deterministic principal mapped to partition 0.
	principalID := "principal-567"
	base := time.Now().UTC().Truncate(time.Second)
	queryStart := base.Add(-1 * time.Minute)
	queryEnd := base.Add(5 * time.Minute)

	var ingestedCount int
	var checkpointAfterBatch int64

	t.Run("health endpoint", func(t *testing.T) {
		resp, err := h.client.Get(h.baseURL + "/health")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	})

	t.Run("ingest first event via canonical endpoint", func(t *testing.T) {
		event := v1.Event{
			ID:          fmt.Sprintf("addon-evt-%d", time.Now().UnixNano()),
			PrincipalID: principalID,
			Type:        "api.request",
			OccurredAt:  base.Add(1 * time.Second),
			Data:        map[string]interface{}{},
		}
		status, body := postJSON(t, h.client, h.baseURL+"/v1/events", event)
		require.Equal(t, http.StatusAccepted, status, string(body))
		ingestedCount++
	})

	t.Run("ingest second event via alias endpoint", func(t *testing.T) {
		event := v1.Event{
			ID:          fmt.Sprintf("addon-evt-%d", time.Now().UnixNano()),
			PrincipalID: principalID,
			Type:        "api.request",
			OccurredAt:  base.Add(2 * time.Second),
			Data:        map[string]interface{}{},
		}
		status, body := postJSON(t, h.client, h.baseURL+"/v1/ingest", event)
		require.Equal(t, http.StatusAccepted, status, string(body))
		ingestedCount++
	})

	t.Run("list raw events endpoint returns both ingested events", func(t *testing.T) {
		listURL := fmt.Sprintf(
			"%s/v1/events/%s?start=%s&end=%s&limit=100",
			h.baseURL,
			principalID,
			base.Add(-1*time.Minute).Format(time.RFC3339),
			base.Add(1*time.Minute).Format(time.RFC3339),
		)

		resp, err := h.client.Get(listURL)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

		var events []v1.Event
		require.NoError(t, json.Unmarshal(body, &events))
		require.Len(t, events, ingestedCount)
	})

	t.Run("trigger cron-batch style pre-aggregation and verify checkpoint", func(t *testing.T) {
		before := readCheckpoint(t, h.db)
		runBatchAggregationOnce(t, h)
		waitForCheckpoint(t, h.db, before+1, 5*time.Second)
		checkpointAfterBatch = readCheckpoint(t, h.db)
		waitForPreAggregateRows(t, h.db, principalID, "count_api_requests", 5*time.Second)
	})

	t.Run("state query leverages pre-aggregates", func(t *testing.T) {
		payload := queryTotalState(t, h, principalID, "count_api_requests", queryStart, queryEnd)
		require.Equal(t, "count_api_requests", payload.Rule)
		require.Len(t, payload.Values, 1)
		require.Equal(t, int64(ingestedCount), payload.Values[0].EventCount)
		require.Equal(t, fmt.Sprintf("%d", ingestedCount), payload.Values[0].Value)
	})

	t.Run("ingest more event then query hybrid pre-aggregate+raw tail", func(t *testing.T) {
		event := v1.Event{
			ID:          fmt.Sprintf("addon-evt-%d", time.Now().UnixNano()),
			PrincipalID: principalID,
			Type:        "api.request",
			OccurredAt:  base.Add(3 * time.Second),
			Data:        map[string]interface{}{},
		}
		status, body := postJSON(t, h.client, h.baseURL+"/v1/events", event)
		require.Equal(t, http.StatusAccepted, status, string(body))
		ingestedCount++

		// No batch triggered here: read path must merge durable pre-aggregates with raw tail events.
		payload := queryTotalState(t, h, principalID, "count_api_requests", queryStart, queryEnd)
		require.Len(t, payload.Values, 1)
		require.Equal(t, int64(ingestedCount), payload.Values[0].EventCount)
		require.Equal(t, fmt.Sprintf("%d", ingestedCount), payload.Values[0].Value)

		after := readCheckpoint(t, h.db)
		require.Equal(t, checkpointAfterBatch, after, "checkpoint should stay unchanged without new batch run")
	})
}

type stateQueryPayload struct {
	Rule   string `json:"rule"`
	Values []struct {
		Value      string `json:"value"`
		EventCount int64  `json:"event_count"`
	} `json:"values"`
}

func queryTotalState(
	t *testing.T,
	h *integrationHarness,
	principalID string,
	rule string,
	start time.Time,
	end time.Time,
) stateQueryPayload {
	t.Helper()

	query := url.Values{}
	query.Set("rule", rule)
	query.Set("start", start.Format(time.RFC3339))
	query.Set("end", end.Format(time.RFC3339))
	query.Set("granularity", "total")

	stateURL := fmt.Sprintf("%s/v1/state/%s?%s", h.baseURL, principalID, query.Encode())
	resp, err := h.client.Get(stateURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	var payload stateQueryPayload
	require.NoError(t, json.Unmarshal(body, &payload))
	return payload
}
