package ingestion

import (
	"github.com/aevon-lab/project-aevon/internal/core/storage"
	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/gin-gonic/gin"
)

type Service struct {
	registry         *schema.Registry
	validator        *schema.Validator
	store            storage.EventStore
	maxBodySizeBytes int
}

func NewService(reg *schema.Registry, val *schema.Validator, repo storage.EventStore, maxBodySizeMB int) *Service {
	if reg == nil {
		panic("ingestion: registry must not be nil")
	}
	if val == nil {
		panic("ingestion: validator must not be nil")
	}
	if repo == nil {
		panic("ingestion: store must not be nil")
	}
	if maxBodySizeMB <= 0 {
		maxBodySizeMB = 1 // default to 1MB
	}
	return &Service{
		registry:         reg,
		validator:        val,
		store:            repo,
		maxBodySizeBytes: maxBodySizeMB * 1024 * 1024,
	}
}

// RegisterRoutes registers the ingestion service routes.
func (s *Service) RegisterRoutes(r gin.IRouter) {
	// Canonical ingestion endpoint.
	r.POST("/v1/events", s.IngestHandler)
	r.GET("/v1/events/:principal_id", s.ListEventsHandler)

	// Backward-compatible alias. Can be removed after clients migrate.
	r.POST("/v1/ingest", s.IngestHandler)
}
