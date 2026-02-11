//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aevon-lab/project-aevon/internal/aggregation"
	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	"github.com/aevon-lab/project-aevon/internal/core/storage/postgres"
	"github.com/aevon-lab/project-aevon/internal/ingestion"
	"github.com/aevon-lab/project-aevon/internal/projection"
	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/protobuf"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/yaml"
	schemaStorage "github.com/aevon-lab/project-aevon/internal/schema/storage"
	"github.com/aevon-lab/project-aevon/internal/server"
	"github.com/stretchr/testify/require"
)

const defaultTestDSN = "postgres://aevon_dev:dev_password@localhost:5432/aevon?sslmode=disable"

type integrationHarness struct {
	baseURL       string
	client        *http.Client
	db            *sql.DB
	cancel        context.CancelFunc
	serverDone    chan error
	schedulerDone chan error
	adapter       *postgres.Adapter
}

func (h *integrationHarness) close(t *testing.T) {
	t.Helper()

	h.cancel()
	select {
	case <-h.serverDone:
	case <-time.After(5 * time.Second):
		t.Log("server shutdown timed out")
	}

	select {
	case <-h.schedulerDone:
	case <-time.After(5 * time.Second):
		t.Log("scheduler shutdown timed out")
	}

	require.NoError(t, h.adapter.Close())
}

func TestCoreAPI_EventsAndState(t *testing.T) {
	h := startHarness(t)
	defer h.close(t)

	require.NoError(t, resetDatabase(t, h.db))

	tenantID := "tenant-integration"
	principalID := "user-integration"
	occurredAt := time.Now().UTC().Truncate(time.Second)
	eventID := fmt.Sprintf("evt-%d", time.Now().UnixNano())

	event := v1.Event{
		ID:          eventID,
		TenantID:    tenantID,
		PrincipalID: principalID,
		Type:        "api.request",
		OccurredAt:  occurredAt,
		Data:        map[string]interface{}{},
	}

	status, body := postJSON(t, h.client, h.baseURL+"/v1/events", event)
	require.Equal(t, http.StatusAccepted, status, string(body))

	query := url.Values{}
	query.Set("rule", "count_api_requests")
	query.Set("start", occurredAt.Add(-1*time.Minute).Format(time.RFC3339))
	query.Set("end", occurredAt.Add(2*time.Minute).Format(time.RFC3339))
	query.Set("granularity", "total")

	stateURL := fmt.Sprintf("%s/v1/state/%s/%s?%s", h.baseURL, tenantID, principalID, query.Encode())
	resp, err := h.client.Get(stateURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(respBody))

	var payload struct {
		Rule   string `json:"rule"`
		Values []struct {
			Value      string `json:"value"`
			EventCount int64  `json:"event_count"`
		} `json:"values"`
	}
	require.NoError(t, json.Unmarshal(respBody, &payload))
	require.Equal(t, "count_api_requests", payload.Rule)
	require.Len(t, payload.Values, 1)
	require.Equal(t, "1", payload.Values[0].Value)
	require.Equal(t, int64(1), payload.Values[0].EventCount)
}

func TestCoreAPI_DuplicateEventReturnsConflict(t *testing.T) {
	h := startHarness(t)
	defer h.close(t)

	require.NoError(t, resetDatabase(t, h.db))

	event := v1.Event{
		ID:          "evt-duplicate-integration",
		TenantID:    "tenant-integration",
		PrincipalID: "user-integration",
		Type:        "api.request",
		OccurredAt:  time.Now().UTC().Truncate(time.Second),
		Data:        map[string]interface{}{},
	}

	status, body := postJSON(t, h.client, h.baseURL+"/v1/events", event)
	require.Equal(t, http.StatusAccepted, status, string(body))

	status, body = postJSON(t, h.client, h.baseURL+"/v1/events", event)
	require.Equal(t, http.StatusConflict, status, string(body))
}

func startHarness(t *testing.T) *integrationHarness {
	t.Helper()

	dsn := os.Getenv("AEVON_TEST_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}

	adapter, err := postgres.NewAdapter(dsn, 10, 10)
	require.NoError(t, err)

	root := projectRoot(t)
	schemaRepo := schemaStorage.NewFileSystemRepository(filepath.Join(root, "schemas"))
	registry := schema.NewRegistry(schemaRepo)

	formatRegistry := schema.NewFormatRegistry()
	formatRegistry.RegisterFormat(schema.FormatProtobuf, protobuf.NewCompiler(), protobuf.NewValidator())
	formatRegistry.RegisterFormat(schema.FormatYaml, yaml.NewCompiler(), yaml.NewValidator())
	validator := schema.NewValidator(formatRegistry)

	ruleRepo, err := aggregation.NewFileSystemRuleRepository(filepath.Join(root, "config", "aggregations"))
	require.NoError(t, err)

	preAggStore := postgres.NewPreAggregateAdapter(adapter.DB())
	ingestionSvc := ingestion.NewService(registry, validator, adapter, 1)
	projectionSvc := projection.NewService(preAggStore, adapter, ruleRepo.GetRules())

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	httpServer := server.New(addr, adapter.DB(), "release")
	ingestionSvc.RegisterRoutes(httpServer.Engine)
	projectionSvc.RegisterRoutes(httpServer.Engine)

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	schedulerDone := make(chan error, 1)

	scheduler := aggregation.NewScheduler(
		200*time.Millisecond,
		adapter,
		preAggStore,
		ruleRepo.GetRules(),
		aggregation.BatchJobOptions{
			BatchSize:   1000,
			WorkerCount: 2,
			BucketSize:  time.Minute,
			BucketLabel: "1m",
		},
	)

	go func() { schedulerDone <- scheduler.Start(ctx) }()
	go func() { serverDone <- httpServer.Run(ctx) }()

	baseURL := "http://" + addr
	waitForHealthy(t, baseURL)

	return &integrationHarness{
		baseURL:       baseURL,
		client:        &http.Client{Timeout: 5 * time.Second},
		db:            adapter.DB(),
		cancel:        cancel,
		serverDone:    serverDone,
		schedulerDone: schedulerDone,
		adapter:       adapter,
	}
}

func waitForHealthy(t *testing.T, baseURL string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("server did not become healthy at %s", baseURL)
}

func postJSON(t *testing.T, client *http.Client, endpoint string, payload interface{}) (int, []byte) {
	t.Helper()

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, respBody
}

func resetDatabase(t *testing.T, db *sql.DB) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `TRUNCATE TABLE pre_aggregates`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `TRUNCATE TABLE events`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `DELETE FROM sweep_checkpoints`)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO sweep_checkpoints (bucket_size, checkpoint_cursor, updated_at)
		VALUES ('1m', 0, NOW())
	`)
	return err
}

func freePort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func projectRoot(t *testing.T) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	return root
}
