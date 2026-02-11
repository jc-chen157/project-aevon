package api

import (
	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/gin-gonic/gin"
)

// Service provides the schema management API.
type Service struct {
	registry  *schema.Registry
	validator *schema.Validator
}

// NewService creates a new schema API service.
func NewService(reg *schema.Registry, val *schema.Validator) *Service {
	return &Service{
		registry:  reg,
		validator: val,
	}
}

// RegisterRoutes registers the schema API routes.
func (s *Service) RegisterRoutes(r gin.IRouter) {
	handler := NewHandler(s.registry, s.validator)

	// Canonical schema listing endpoint for MVP clients.
	r.GET("/v1/schema", handler.HandleList)

	schemas := r.Group("/v1/schemas")
	{
		schemas.GET("", handler.HandleList)
		// /v1/schemas/{type}/{version}
		schemas.GET("/:type/:version", handler.HandleGet)
		schemas.POST("/:type/:version/validate", handler.HandleValidate)
	}
}
