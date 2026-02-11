package api

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

// Handler handles schema management HTTP requests.
type Handler struct {
	registry  *schema.Registry
	validator *schema.Validator
}

// NewHandler creates a new schema API handler.
func NewHandler(reg *schema.Registry, val *schema.Validator) *Handler {
	return &Handler{
		registry:  reg,
		validator: val,
	}
}

// RegisterSchemaRequest is the request body for POST /v1/schemas.
type RegisterSchemaRequest struct {
	Type       string `json:"type"`
	Version    int    `json:"version"`
	Definition string `json:"definition"` // Base64 or raw proto content
	StrictMode *bool  `json:"strict_mode,omitempty"`
}

// SchemaResponse is the response body for schema operations.
type SchemaResponse struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	Type        string `json:"type"`
	Version     int    `json:"version"`
	Format      string `json:"format"`
	State       string `json:"state"`
	StrictMode  bool   `json:"strict_mode"`
	Fingerprint string `json:"fingerprint"`
	CreatedAt   string `json:"created_at"`
}

// ListedSchemaResponse is the list payload for schema discovery endpoints.
// For YAML schemas, Definition contains parsed JSON-compatible data.
type ListedSchemaResponse struct {
	TenantID   string      `json:"tenant_id"`
	Type       string      `json:"type"`
	Version    int         `json:"version"`
	Format     string      `json:"format"`
	StrictMode bool        `json:"strict_mode"`
	State      string      `json:"state"`
	Definition interface{} `json:"definition"`
}

// ErrorResponse is the error response body.
type ErrorResponse struct {
	Error   string      `json:"error"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
}

// HandleGet handles GET /v1/schemas/{type}/{version}.
func (h *Handler) HandleGet(c *gin.Context) {
	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "missing_tenant", Message: "X-Tenant-ID header is required"})
		return
	}

	eventType := c.Param("type")
	versionStr := c.Param("version")
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid_version", Message: "version must be an integer"})
		return
	}

	s, err := h.registry.Get(c.Request.Context(), tenantID, eventType, version)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "schema_not_found", Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, h.toResponse(s))
}

// HandleList handles GET /v1/schemas.
func (h *Handler) HandleList(c *gin.Context) {
	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "missing_tenant", Message: "X-Tenant-ID header is required"})
		return
	}

	// Optional filter by type
	eventType := c.Query("type")

	schemas, err := h.registry.List(c.Request.Context(), tenantID, eventType)
	if err != nil {
		slog.Error("Schema list error", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "internal_error", Message: "Failed to list schemas"})
		return
	}

	responses := make([]*ListedSchemaResponse, len(schemas))
	for i, s := range schemas {
		resp, convErr := h.toListedResponse(s)
		if convErr != nil {
			slog.Error("Schema list conversion error", "error", convErr, "type", s.Type, "version", s.Version)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "internal_error", Message: "Failed to convert schema definition"})
			return
		}
		responses[i] = resp
	}

	c.JSON(http.StatusOK, responses)
}

// HandleValidate handles POST /v1/schemas/{type}/{version}/validate (dry-run).
func (h *Handler) HandleValidate(c *gin.Context) {
	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "missing_tenant", Message: "X-Tenant-ID header is required"})
		return
	}

	eventType := c.Param("type")
	versionStr := c.Param("version")
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid_version", Message: "version must be an integer"})
		return
	}

	var data map[string]interface{}
	if err := c.ShouldBindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid_json", Message: "Invalid JSON body"})
		return
	}

	s, err := h.registry.Get(c.Request.Context(), tenantID, eventType, version)
	if err != nil {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "schema_not_found", Message: err.Error()})
		return
	}

	if err := h.validator.ValidateData(c.Request.Context(), s, data); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "validation_failed", Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"valid":   true,
		"schema":  eventType,
		"version": version,
	})
}

func (h *Handler) toResponse(s *schema.Schema) *SchemaResponse {
	return &SchemaResponse{
		ID:          s.ID,
		TenantID:    s.TenantID,
		Type:        s.Type,
		Version:     s.Version,
		Format:      string(s.Format),
		State:       string(s.State),
		StrictMode:  s.StrictMode,
		Fingerprint: s.Fingerprint,
		CreatedAt:   s.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}

func (h *Handler) toListedResponse(s *schema.Schema) (*ListedSchemaResponse, error) {
	resp := &ListedSchemaResponse{
		TenantID:   s.TenantID,
		Type:       s.Type,
		Version:    s.Version,
		Format:     string(s.Format),
		StrictMode: s.StrictMode,
		State:      string(s.State),
	}

	if s.Format == schema.FormatYaml {
		var parsed map[string]interface{}
		if err := yaml.Unmarshal(s.Definition, &parsed); err != nil {
			return nil, err
		}
		resp.Definition = parsed
		return resp, nil
	}

	resp.Definition = map[string]interface{}{
		"raw": string(s.Definition),
	}
	return resp, nil
}
