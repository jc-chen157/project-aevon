package errors

const (
	HttpInternalError         = "internal_error"
	HttpInvalidJsonError      = "invalid_json"
	HttpSchemaNotFoundError   = "schema_not_found"
	HttpSchemaValidationError = "schema_validation_failed"
	HttpDuplicateEventError   = "duplicate_event"
)

// ErrorResponse is the error response body for ingestion errors.
type ErrorResponse struct {
	ErrorType string      `json:"error_type"`
	Message   string      `json:"message"`
	Details   interface{} `json:"details,omitempty"`
}
