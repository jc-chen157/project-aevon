package postgres

import (
	"encoding/json"
	"fmt"

	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
)

// marshalEventJSON marshals an event's metadata and data fields to JSON.
// Returns metadata and data as JSON bytes, handling nil metadata gracefully.
//
// Nil metadata produces nil (SQL NULL) rather than JSON "null" string.
func marshalEventJSON(event *v1.Event) (metadataJSON, dataJSON []byte, err error) {
	if event.Metadata != nil && len(event.Metadata) > 0 {
		metadataJSON, err = json.Marshal(event.Metadata)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	dataJSON, err = json.Marshal(event.Data)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	return metadataJSON, dataJSON, nil
}

type scanner interface {
	Scan(dest ...interface{}) error
}

// scanEventRow scans a database row into an Event struct.
// Handles JSON unmarshalling for metadata and data fields.
// Compatible with both sql.Row (single) and sql.Rows (multiple).
func scanEventRow(row scanner) (*v1.Event, error) {
	var evt v1.Event
	var metadataJSON, dataJSON []byte

	err := row.Scan(
		&evt.ID,
		&evt.TenantID,
		&evt.PrincipalID,
		&evt.Type,
		&evt.SchemaVersion,
		&evt.OccurredAt,
		&evt.IngestedAt,
		&metadataJSON,
		&dataJSON,
		&evt.IngestSeq,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to scan event row: %w", err)
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &evt.Metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}
	}

	if err := json.Unmarshal(dataJSON, &evt.Data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return &evt, nil
}
