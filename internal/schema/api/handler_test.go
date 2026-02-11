package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aevon-lab/project-aevon/internal/schema"
	schemaStorage "github.com/aevon-lab/project-aevon/internal/schema/storage"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestHandleList_ReturnsArrayWithJSONDefinitions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	root := t.TempDir()
	schemaDir := filepath.Join(root, "tenant-a", "api.request")
	require.NoError(t, os.MkdirAll(schemaDir, 0o755))

	definition := `
event: api.request
version: 1
description: HTTP API request tracking event
strictMode: true
fields:
  request_id: string!
  timestamp: int64!
`
	require.NoError(t, os.WriteFile(filepath.Join(schemaDir, "v1.yaml"), []byte(definition), 0o644))

	registry := schema.NewRegistry(schemaStorage.NewFileSystemRepository(root))
	validator := schema.NewValidator(schema.NewFormatRegistry())
	svc := NewService(registry, validator)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodGet, "/v1/schema", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)

	var body []map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &body))
	require.Len(t, body, 1)
	require.Equal(t, "tenant-a", body[0]["tenant_id"])
	require.Equal(t, "api.request", body[0]["type"])
	require.Equal(t, float64(1), body[0]["version"])
	require.Equal(t, "yaml", body[0]["format"])

	defMap, ok := body[0]["definition"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "api.request", defMap["event"])
	require.Equal(t, float64(1), defMap["version"])
}

func TestHandleList_RequiresTenantHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)

	registry := schema.NewRegistry(schemaStorage.NewFileSystemRepository(t.TempDir()))
	validator := schema.NewValidator(schema.NewFormatRegistry())
	svc := NewService(registry, validator)

	r := gin.New()
	svc.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodGet, "/v1/schema", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	require.Equal(t, http.StatusBadRequest, resp.Code)

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &body))
	require.Equal(t, "missing_tenant", body["error"])
}
