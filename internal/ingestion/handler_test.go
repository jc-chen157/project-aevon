package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	httperr "github.com/aevon-lab/project-aevon/internal/core/errors"
	"github.com/aevon-lab/project-aevon/internal/core/storage"
	storagemocks "github.com/aevon-lab/project-aevon/internal/mocks/storage"
	internalschema "github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type errReadCloser struct{}

func (errReadCloser) Read(_ []byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (errReadCloser) Close() error               { return nil }

func TestIngestHandler_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Prepare test event
	evt := &v1.Event{
		ID:            "evt-001",
		PrincipalID:   "user-1",
		Type:          "api.request",
		SchemaVersion: 0, // Skip schema validation
		OccurredAt:    time.Now().UTC(),
		Data:          map[string]interface{}{"count": 1},
	}

	body, _ := json.Marshal(evt)

	// Mock storage
	mockStore := storagemocks.NewEventStore(t)
	mockStore.EXPECT().
		SaveEvent(mock.Anything, mock.MatchedBy(func(e *v1.Event) bool {
			return e.ID == "evt-001"
		})).
		Return(nil).
		Once()

	// Create service
	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	// Setup router
	r := gin.New()
	svc.RegisterRoutes(r)

	// Execute request
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	// Assert
	require.Equal(t, http.StatusAccepted, resp.Code)
	var result map[string]string
	json.Unmarshal(resp.Body.Bytes(), &result)
	require.Equal(t, "accepted", result["status"])
}

func TestIngestHandler_InvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockStore := storagemocks.NewEventStore(t)
	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	// Send malformed JSON
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)

	var errResp httperr.ErrorResponse
	json.Unmarshal(resp.Body.Bytes(), &errResp)
	require.Equal(t, httperr.HttpInvalidJsonError, errResp.ErrorType)
}

func TestIngestHandler_ValidationFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Event missing required fields
	evt := &v1.Event{
		ID: "evt-001",
		// Missing PrincipalID
		Type:       "api.request",
		OccurredAt: time.Now().UTC(),
		Data:       map[string]interface{}{"count": 1},
	}

	body, _ := json.Marshal(evt)

	mockStore := storagemocks.NewEventStore(t)
	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)

	var errResp httperr.ErrorResponse
	json.Unmarshal(resp.Body.Bytes(), &errResp)
	require.Equal(t, httperr.HttpInvalidJsonError, errResp.ErrorType)
}

func TestIngestHandler_DuplicateEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)

	evt := &v1.Event{
		ID:            "evt-001",
		PrincipalID:   "user-1",
		Type:          "api.request",
		SchemaVersion: 0,
		OccurredAt:    time.Now().UTC(),
		Data:          map[string]interface{}{"count": 1},
	}

	body, _ := json.Marshal(evt)

	// Mock storage to return duplicate error
	mockStore := storagemocks.NewEventStore(t)
	mockStore.EXPECT().
		SaveEvent(mock.Anything, mock.Anything).
		Return(storage.ErrDuplicate).
		Once()

	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusConflict, resp.Code)

	var errResp httperr.ErrorResponse
	json.Unmarshal(resp.Body.Bytes(), &errResp)
	require.Equal(t, httperr.HttpDuplicateEventError, errResp.ErrorType)
}

func TestIngestHandler_StorageError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	evt := &v1.Event{
		ID:            "evt-001",
		PrincipalID:   "user-1",
		Type:          "api.request",
		SchemaVersion: 0,
		OccurredAt:    time.Now().UTC(),
		Data:          map[string]interface{}{"count": 1},
	}

	body, _ := json.Marshal(evt)

	// Mock storage to return generic error
	mockStore := storagemocks.NewEventStore(t)
	mockStore.EXPECT().
		SaveEvent(mock.Anything, mock.Anything).
		Return(errors.New("database connection failed")).
		Once()

	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusInternalServerError, resp.Code)

	var errResp httperr.ErrorResponse
	json.Unmarshal(resp.Body.Bytes(), &errResp)
	require.Equal(t, httperr.HttpInternalError, errResp.ErrorType)
}

func TestIngestHandler_SchemaNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	evt := &v1.Event{
		ID:            "evt-001",
		PrincipalID:   "user-1",
		Type:          "api.request",
		SchemaVersion: 999, // Non-existent schema version
		OccurredAt:    time.Now().UTC(),
		Data:          map[string]interface{}{"field": "value"},
	}

	body, _ := json.Marshal(evt)

	mockStore := storagemocks.NewEventStore(t)

	// Mock schema registry to return error (schema not found)
	mockRepo := &mockSchemaRepo{
		err: errors.New("schema not found"),
	}
	registry := internalschema.NewRegistry(mockRepo)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())

	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)

	var errResp httperr.ErrorResponse
	json.Unmarshal(resp.Body.Bytes(), &errResp)
	require.Equal(t, httperr.HttpSchemaNotFoundError, errResp.ErrorType)
}

func TestIngestHandler_SchemaValidationFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	evt := &v1.Event{
		ID:            "evt-schema-validation-failure",
		PrincipalID:   "user-1",
		Type:          "api.request",
		SchemaVersion: 1,
		OccurredAt:    time.Now().UTC(),
		Data:          map[string]interface{}{"field": "value"},
	}
	body, _ := json.Marshal(evt)

	mockStore := storagemocks.NewEventStore(t)

	// Return a schema so ingestion reaches validator.ValidateData path.
	// We intentionally use a validator without registered YAML compiler/validator,
	// which forces ValidateData to fail and exercises schema validation error mapping.
	mockRepo := &mockSchemaRepo{
		schema: &internalschema.Schema{
			TenantID:    "default",
			Type:        "api.request",
			Version:     1,
			Format:      internalschema.FormatYaml,
			Definition:  []byte("event: api.request\nversion: 1\nfields:\n  field: string\n"),
			Fingerprint: "fp-1",
			State:       internalschema.StateActive,
			CreatedAt:   time.Now().UTC(),
		},
	}
	registry := internalschema.NewRegistry(mockRepo)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)

	var errResp httperr.ErrorResponse
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &errResp))
	require.Equal(t, httperr.HttpSchemaValidationError, errResp.ErrorType)
	require.Contains(t, errResp.Message, "compilation failed")
}

func TestIngestHandler_BodySizeLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockStore := storagemocks.NewEventStore(t)
	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())

	// Set very small body size limit (1 byte)
	svc := NewService(registry, validator, mockStore, 0) // 0 defaults to 1MB, but we'll test with custom
	svc.maxBodySizeBytes = 10                            // Very small limit

	r := gin.New()
	svc.RegisterRoutes(r)

	// Create payload larger than limit
	largePayload := map[string]interface{}{
		"data": "this is definitely more than 10 bytes of content",
	}
	body, _ := json.Marshal(largePayload)

	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, resp.Code)

	var errResp httperr.ErrorResponse
	json.Unmarshal(resp.Body.Bytes(), &errResp)
	require.Equal(t, httperr.HttpInvalidJsonError, errResp.ErrorType)
	require.Contains(t, errResp.Message, "maximum allowed size")
}

func TestListEventsHandler_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	start := time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)

	mockStore := storagemocks.NewEventStore(t)
	mockStore.EXPECT().
		RetrieveEventsByPrincipalAndIngestedRange(mock.Anything, "user-1", start, end, 1000).
		Return([]*v1.Event{
			{
				ID:          "evt-1",
				PrincipalID: "user-1",
				Type:        "api.request",
				OccurredAt:  start,
				IngestedAt:  start.Add(time.Second),
				Data:        map[string]interface{}{"count": float64(1)},
			},
		}, nil).
		Once()

	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/events/user-1?start="+start.Format(time.RFC3339)+"&end="+end.Format(time.RFC3339),
		nil,
	)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var events []v1.Event
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &events))
	require.Len(t, events, 1)
	require.Equal(t, "evt-1", events[0].ID)
	require.Equal(t, "user-1", events[0].PrincipalID)
}

func TestListEventsHandler_InvalidQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockStore := storagemocks.NewEventStore(t)
	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	start := time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC)
	end := start.Add(-1 * time.Minute)
	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/events/user-1?start="+start.Format(time.RFC3339)+"&end="+end.Format(time.RFC3339),
		nil,
	)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestListEventsHandler_InvalidQueryBinding(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockStore := storagemocks.NewEventStore(t)
	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/events/user-1?start=not-a-time&end=2026-02-08T10:10:00Z",
		nil,
	)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)
	var errResp httperr.ErrorResponse
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &errResp))
	require.Equal(t, httperr.HttpInvalidJsonError, errResp.ErrorType)
	require.Equal(t, "Invalid query parameters", errResp.Message)
}

func TestListEventsHandler_InvalidLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []string{
		"/v1/events/user-1?start=2026-02-08T10:00:00Z&end=2026-02-08T10:10:00Z&limit=-1",
		"/v1/events/user-1?start=2026-02-08T10:00:00Z&end=2026-02-08T10:10:00Z&limit=5001",
	}

	for _, path := range tests {
		t.Run(path, func(t *testing.T) {
			mockStore := storagemocks.NewEventStore(t)
			registry := internalschema.NewRegistry(nil)
			validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
			svc := NewService(registry, validator, mockStore, 1)

			r := gin.New()
			svc.RegisterRoutes(r)

			req := httptest.NewRequest(http.MethodGet, path, nil)
			resp := httptest.NewRecorder()
			r.ServeHTTP(resp, req)

			require.Equal(t, http.StatusBadRequest, resp.Code)
			var errResp httperr.ErrorResponse
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &errResp))
			require.Equal(t, httperr.HttpInvalidJsonError, errResp.ErrorType)
			require.Equal(t, "Invalid query parameters", errResp.Message)
		})
	}
}

func TestListEventsHandler_StoreError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	start := time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)

	mockStore := storagemocks.NewEventStore(t)
	mockStore.EXPECT().
		RetrieveEventsByPrincipalAndIngestedRange(mock.Anything, "user-1", start, end, 100).
		Return(nil, errors.New("db failure")).
		Once()

	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/events/user-1?start="+start.Format(time.RFC3339)+"&end="+end.Format(time.RFC3339)+"&limit=100",
		nil,
	)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusInternalServerError, resp.Code)
}

func TestService_ParseEvent_ReadBodyError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	mockStore := storagemocks.NewEventStore(t)
	registry := internalschema.NewRegistry(nil)
	validator := internalschema.NewValidator(internalschema.NewFormatRegistry())
	svc := NewService(registry, validator, mockStore, 1)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodPost, "/v1/events", nil)
	req.Body = errReadCloser{}
	c.Request = req

	evt, payloadSize, err := svc.parseEvent(c)
	require.Nil(t, evt)
	require.Equal(t, 0, payloadSize)
	require.NotNil(t, err)
	require.Equal(t, http.StatusInternalServerError, err.statusCode)
	require.Equal(t, httperr.HttpInternalError, err.errorType)
	require.Equal(t, msgReadBodyFailed, err.message)
}

// mockSchemaRepo is a simple in-memory schema repository for testing
type mockSchemaRepo struct {
	schema *internalschema.Schema
	err    error
}

func (m *mockSchemaRepo) Create(ctx context.Context, schema *internalschema.Schema) error {
	return nil
}

func (m *mockSchemaRepo) Get(ctx context.Context, key internalschema.Key) (*internalschema.Schema, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.schema, nil
}

func (m *mockSchemaRepo) List(ctx context.Context, tenantID, eventType string) ([]*internalschema.Schema, error) {
	return nil, nil
}

func (m *mockSchemaRepo) UpdateState(ctx context.Context, key internalschema.Key, state internalschema.State) error {
	return nil
}

func (m *mockSchemaRepo) Delete(ctx context.Context, key internalschema.Key) error {
	return nil
}
