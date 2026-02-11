package projection

import (
	"errors"
	"net/http"
	"time"

	httperr "github.com/aevon-lab/project-aevon/internal/core/errors"
	"github.com/gin-gonic/gin"
)

// RegisterRoutes registers all projection API routes on the given router.
func (s *Service) RegisterRoutes(r gin.IRouter) {
	// Canonical state query endpoint.
	r.GET("/v1/state/:tenant_id/:principal_id", s.HandleQueryAggregates)

	// Backward-compatible alias. Can be removed after clients migrate.
	r.GET("/v1/aggregates/:tenant_id/:principal_id", s.HandleQueryAggregates)
}

// HandleQueryAggregates handles GET /v1/state/:tenant_id/:principal_id
// Query parameters: rule, start, end, granularity
func (s *Service) HandleQueryAggregates(c *gin.Context) {
	var uri struct {
		TenantID    string `uri:"tenant_id" binding:"required"`
		PrincipalID string `uri:"principal_id" binding:"required"`
	}
	var query struct {
		Rule        string    `form:"rule" binding:"required"`
		Start       time.Time `form:"start" binding:"required" time_format:"2006-01-02T15:04:05Z07:00"`
		End         time.Time `form:"end" binding:"required" time_format:"2006-01-02T15:04:05Z07:00"`
		Granularity string    `form:"granularity"`
	}

	// Bind URI parameters (tenant_id, principal_id)
	if err := c.ShouldBindUri(&uri); err != nil {
		c.JSON(http.StatusBadRequest, httperr.ErrorResponse{
			ErrorType: httperr.HttpInvalidJsonError,
			Message:   "Invalid path parameters",
			Details:   err.Error(),
		})
		return
	}

	// Bind query parameters (rule, start, end, granularity)
	if err := c.ShouldBindQuery(&query); err != nil {
		c.JSON(http.StatusBadRequest, httperr.ErrorResponse{
			ErrorType: httperr.HttpInvalidJsonError,
			Message:   "Invalid query parameters",
			Details:   err.Error(),
		})
		return
	}

	req := AggregateQueryRequest{
		TenantID:    uri.TenantID,
		PrincipalID: uri.PrincipalID,
		Rule:        query.Rule,
		Start:       query.Start,
		End:         query.End,
		Granularity: query.Granularity,
	}

	// Execute query
	resp, err := s.QueryAggregates(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, ErrInvalidQuery) {
			c.JSON(http.StatusBadRequest, httperr.ErrorResponse{
				ErrorType: httperr.HttpInvalidJsonError,
				Message:   "Invalid aggregate query",
				Details:   err.Error(),
			})
			return
		}

		c.JSON(http.StatusInternalServerError, httperr.ErrorResponse{
			ErrorType: httperr.HttpInternalError,
			Message:   "Failed to query aggregates",
			Details:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}
