package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
