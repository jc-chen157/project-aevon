package ingestion

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	httperr "github.com/aevon-lab/project-aevon/internal/core/errors"
	"github.com/aevon-lab/project-aevon/internal/core/storage"
	"github.com/aevon-lab/project-aevon/internal/schema"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	"github.com/gin-gonic/gin"
)

const (
	msgReadBodyFailed = "Failed to read request body"
	msgInvalidJSON    = "Invalid JSON body"
	msgPersistFailed  = "Failed to persist event"
	msgDuplicateEvent = "Event already exists"
)

// ingestionError carries the structured HTTP error shape from a helper back to the orchestrator.
// Helpers return this instead of writing to gin.Context directly, keeping them decoupled from HTTP.
type ingestionError struct {
	statusCode int
	errorType  string
	message    string
	details    interface{}
}

func (e *ingestionError) Error() string {
	return e.message
}

// IngestHandler handles HTTP POST requests for event ingestion.
func (s *Service) IngestHandler(c *gin.Context) {
	evt, payloadSize, err := s.parseEvent(c)
	if err != nil {
		writeError(c, err)
		return
	}

	if err := s.validateEvent(c.Request.Context(), evt); err != nil {
		writeError(c, err)
		return
	}

	slog.Info("Received Event",
		"event_id", evt.ID,
		"principal_id", evt.PrincipalID,
		"event_type", evt.Type,
		"schema_version", evt.SchemaVersion,
		"payload_size", payloadSize)

	if err := s.persistEvent(c.Request.Context(), evt); err != nil {
		writeError(c, err)
		return
	}

	// Event persisted to DB. Cron batch job will pick it up on next cycle.
	c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
}

// parseEvent reads the raw request body and binds it into an Event struct.
// Returns the parsed event and the raw payload size (used for structured logging upstream).
func (s *Service) parseEvent(c *gin.Context) (*v1.Event, int, *ingestionError) {
	// Enforce maximum body size to prevent OOM attacks
	maxBytes := int64(s.maxBodySizeBytes)
	limitedBody := io.LimitReader(c.Request.Body, maxBytes+1) // +1 to detect oversized requests

	bodyBytes, err := io.ReadAll(limitedBody)
	if err != nil {
		slog.Error("Failed to read request body", "error", err)
		return nil, 0, &ingestionError{
			statusCode: http.StatusInternalServerError,
			errorType:  httperr.HttpInternalError,
			message:    msgReadBodyFailed,
		}
	}

	// Check if body exceeds maximum size
	if int64(len(bodyBytes)) > maxBytes {
		slog.Warn("Request body exceeds maximum size", "size", len(bodyBytes), "max", maxBytes)
		return nil, len(bodyBytes), &ingestionError{
			statusCode: http.StatusRequestEntityTooLarge,
			errorType:  httperr.HttpInvalidJsonError,
			message:    "Request body exceeds maximum allowed size",
			details: map[string]interface{}{
				"max_size_mb": maxBytes / (1024 * 1024),
			},
		}
	}

	c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	var evt v1.Event
	if err := c.ShouldBindJSON(&evt); err != nil {
		slog.Warn("Invalid JSON body received", "error", err, "payload_size", len(bodyBytes))
		return nil, len(bodyBytes), &ingestionError{
			statusCode: http.StatusBadRequest,
			errorType:  httperr.HttpInvalidJsonError,
			message:    msgInvalidJSON,
		}
	}

	// set IngestedAt to be the time we receive the request
	evt.IngestedAt = time.Now().UTC()
	return &evt, len(bodyBytes), nil
}

// validateEvent runs envelope validation, then schema validation if a registry is configured
// and the event declares a SchemaVersion. Returns nil on success.
func (s *Service) validateEvent(ctx context.Context, evt *v1.Event) *ingestionError {
	if err := evt.Validate(); err != nil {
		slog.Warn("Envelope validation failed", "error", err, "event_id", evt.ID)
		return &ingestionError{
			statusCode: http.StatusBadRequest,
			errorType:  httperr.HttpInvalidJsonError,
			message:    err.Error(),
		}
	}

	if evt.SchemaVersion == 0 {
		return nil
	}

	// Use "default" as tenant_id for schema registry (can be updated later)
	sch, err := s.registry.Get(ctx, "default", evt.Type, evt.SchemaVersion)
	if err != nil {
		slog.Warn("Schema not found for event", "event_type", evt.Type, "schema_version", evt.SchemaVersion, "error", err)
		return &ingestionError{
			statusCode: http.StatusBadRequest,
			errorType:  httperr.HttpSchemaNotFoundError,
			message:    err.Error(),
		}
	}

	if sch.State == schema.StateDeprecated {
		slog.Warn("Using deprecated schema", "event_type", evt.Type, "schema_version", evt.SchemaVersion)
	}

	if err := s.validator.ValidateData(ctx, sch, evt.Data); err != nil {
		slog.Warn("Schema validation failed for event data", "event_id", evt.ID, "event_type", evt.Type, "schema_version", evt.SchemaVersion, "error", err)

		details := map[string]interface{}{
			"schema":  evt.Type,
			"version": evt.SchemaVersion,
		}
		if d, ok := err.(schema.ValidationDetailer); ok {
			for k, v := range d.Details() {
				details[k] = v
			}
		}

		return &ingestionError{
			statusCode: http.StatusBadRequest,
			errorType:  httperr.HttpSchemaValidationError,
			message:    err.Error(),
			details:    details,
		}
	}

	return nil
}

// persistEvent saves the event to the backing store.
func (s *Service) persistEvent(ctx context.Context, evt *v1.Event) *ingestionError {
	if err := s.store.SaveEvent(ctx, evt); err != nil {
		if errors.Is(err, storage.ErrDuplicate) {
			slog.Info("Duplicate event rejected", "event_id", evt.ID, "principal_id", evt.PrincipalID)
			return &ingestionError{
				statusCode: http.StatusConflict,
				errorType:  httperr.HttpDuplicateEventError,
				message:    msgDuplicateEvent,
			}
		}

		slog.Error("Failed to persist event", "error", err, "event_id", evt.ID)
		return &ingestionError{
			statusCode: http.StatusInternalServerError,
			errorType:  httperr.HttpInternalError,
			message:    msgPersistFailed,
		}
	}

	return nil
}

// writeError serializes an ingestionError as the JSON HTTP response.
func writeError(c *gin.Context, err *ingestionError) {
	c.JSON(err.statusCode, httperr.ErrorResponse{
		ErrorType: err.errorType,
		Message:   err.message,
		Details:   err.details,
	})
}
