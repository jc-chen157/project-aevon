package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	v1 "github.com/aevon-lab/project-aevon/internal/api/v1"
	"github.com/aevon-lab/project-aevon/internal/core/storage"
	"github.com/stretchr/testify/require"
)

func TestAdapter_SaveEvent(t *testing.T) {
	now := time.Date(2026, 2, 8, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		event          *v1.Event
		mockResult     func(mock sqlmock.Sqlmock, event *v1.Event)
		assertions     func(t *testing.T, event *v1.Event, err error)
		expectationsOK bool
	}{
		{
			name: "success sets ingest seq",
			event: &v1.Event{
				ID:            "evt-1",
				TenantID:      "tenant-1",
				PrincipalID:   "user-1",
				Type:          "api.request",
				SchemaVersion: 1,
				OccurredAt:    now,
				IngestedAt:    now,
				Metadata:      map[string]string{"source": "api"},
				Data:          map[string]interface{}{"count": 3},
			},
			mockResult: func(mock sqlmock.Sqlmock, event *v1.Event) {
				mock.ExpectQuery(regexp.QuoteMeta(querySaveEvent)).
					WithArgs(
						event.ID,
						event.TenantID,
						event.PrincipalID,
						event.Type,
						event.SchemaVersion,
						event.OccurredAt,
						event.IngestedAt,
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
					).
					WillReturnRows(sqlmock.NewRows([]string{"ingest_seq"}).AddRow(int64(42)))
			},
			assertions: func(t *testing.T, event *v1.Event, err error) {
				require.NoError(t, err)
				require.Equal(t, int64(42), event.IngestSeq)
			},
			expectationsOK: true,
		},
		{
			name: "duplicate maps to ErrDuplicate",
			event: &v1.Event{
				ID:            "evt-dup",
				TenantID:      "tenant-1",
				PrincipalID:   "user-1",
				Type:          "api.request",
				SchemaVersion: 1,
				OccurredAt:    now,
				IngestedAt:    now,
				Data:          map[string]interface{}{"count": 1},
			},
			mockResult: func(mock sqlmock.Sqlmock, event *v1.Event) {
				mock.ExpectQuery(regexp.QuoteMeta(querySaveEvent)).
					WithArgs(
						event.ID,
						event.TenantID,
						event.PrincipalID,
						event.Type,
						event.SchemaVersion,
						event.OccurredAt,
						event.IngestedAt,
						sqlmock.AnyArg(),
						sqlmock.AnyArg(),
					).
					WillReturnRows(sqlmock.NewRows([]string{"ingest_seq"}))
			},
			assertions: func(t *testing.T, event *v1.Event, err error) {
				require.ErrorIs(t, err, storage.ErrDuplicate)
				require.Equal(t, int64(0), event.IngestSeq)
			},
			expectationsOK: true,
		},
		{
			name: "marshal error short-circuits",
			event: &v1.Event{
				ID:            "evt-bad",
				TenantID:      "tenant-1",
				PrincipalID:   "user-1",
				Type:          "api.request",
				SchemaVersion: 1,
				OccurredAt:    now,
				IngestedAt:    now,
				Data:          map[string]interface{}{"value": math.NaN()},
			},
			assertions: func(t *testing.T, event *v1.Event, err error) {
				require.Error(t, err)
				require.ErrorContains(t, err, "failed to marshal data")
			},
			expectationsOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter, mock, db := newMockAdapter(t)
			defer db.Close()

			if tc.mockResult != nil {
				tc.mockResult(mock, tc.event)
			}

			err := adapter.SaveEvent(context.Background(), tc.event)
			tc.assertions(t, tc.event, err)

			if tc.expectationsOK {
				require.NoError(t, mock.ExpectationsWereMet())
			}
		})
	}
}

func TestAdapter_RetrieveEventsAfterCursor(t *testing.T) {
	adapter, mock, db := newMockAdapter(t)
	defer db.Close()

	occurredAt := time.Date(2026, 2, 8, 10, 0, 0, 0, time.UTC)
	ingestedAt := occurredAt.Add(2 * time.Second)

	mock.ExpectQuery(regexp.QuoteMeta(queryRetrieveEventsAfterCursor)).
		WithArgs(int64(100), 2).
		WillReturnRows(sqlmock.NewRows(eventRowColumns()).
			AddRow(
				"evt-101",
				"tenant-1",
				"user-1",
				"api.request",
				1,
				occurredAt,
				ingestedAt,
				[]byte(`{"source":"api"}`),
				[]byte(`{"count":3}`),
				int64(101),
			).
			AddRow(
				"evt-102",
				"tenant-1",
				"user-1",
				"api.request",
				1,
				occurredAt.Add(time.Minute),
				ingestedAt.Add(time.Minute),
				[]byte(`{"source":"worker"}`),
				[]byte(`{"count":4}`),
				int64(102),
			),
		).RowsWillBeClosed()

	events, err := adapter.RetrieveEventsAfterCursor(context.Background(), 100, 2)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "evt-101", events[0].ID)
	require.Equal(t, int64(101), events[0].IngestSeq)
	require.Equal(t, "api", events[0].Metadata["source"])
	require.Equal(t, float64(3), events[0].Data["count"])
	require.Equal(t, "evt-102", events[1].ID)
	require.Equal(t, int64(102), events[1].IngestSeq)
	require.Equal(t, "worker", events[1].Metadata["source"])
	require.Equal(t, float64(4), events[1].Data["count"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAdapter_RetrieveScopedEventsAfterCursor(t *testing.T) {
	adapter, mock, db := newMockAdapter(t)
	defer db.Close()

	start := time.Date(2026, 2, 8, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	mock.ExpectQuery(regexp.QuoteMeta(queryRetrieveScopedEventsAfterCursor)).
		WithArgs(int64(42), "tenant-1", "user-1", "api.request", start, end, 5000).
		WillReturnRows(sqlmock.NewRows(eventRowColumns()).
			AddRow(
				"evt-43",
				"tenant-1",
				"user-1",
				"api.request",
				1,
				start.Add(10*time.Minute),
				start.Add(10*time.Minute).Add(time.Second),
				[]byte(`{"trace_id":"trace-1"}`),
				[]byte(`{"count":1}`),
				int64(43),
			),
		).RowsWillBeClosed()

	events, err := adapter.RetrieveScopedEventsAfterCursor(
		context.Background(),
		42,
		"tenant-1",
		"user-1",
		"api.request",
		start,
		end,
		5000,
	)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "evt-43", events[0].ID)
	require.Equal(t, int64(43), events[0].IngestSeq)
	require.Equal(t, "trace-1", events[0].Metadata["trace_id"])
	require.Equal(t, float64(1), events[0].Data["count"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAdapter_CloseReturnsDBCloseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	dbCloseErr := errors.New("db close failed")

	mock.ExpectPrepare(regexp.QuoteMeta(querySaveEvent)).WillBeClosed()
	stmtSave, err := db.Prepare(querySaveEvent)
	require.NoError(t, err)

	mock.ExpectPrepare(regexp.QuoteMeta(queryRetrieveEventsAfter)).WillBeClosed()
	stmtRetrieve, err := db.Prepare(queryRetrieveEventsAfter)
	require.NoError(t, err)

	mock.ExpectPrepare(regexp.QuoteMeta(queryRetrieveEventsAfterCursor)).WillBeClosed()
	stmtRetrieveCursor, err := db.Prepare(queryRetrieveEventsAfterCursor)
	require.NoError(t, err)

	mock.ExpectPrepare(regexp.QuoteMeta(queryRetrieveScopedEventsAfterCursor)).WillBeClosed()
	stmtRetrieveScopedCursor, err := db.Prepare(queryRetrieveScopedEventsAfterCursor)
	require.NoError(t, err)

	mock.ExpectClose().WillReturnError(dbCloseErr)

	adapter := &Adapter{
		db:                       db,
		stmtSaveEvent:            stmtSave,
		stmtRetrieveEvents:       stmtRetrieve,
		stmtRetrieveEventsCursor: stmtRetrieveCursor,
		stmtRetrieveScopedCursor: stmtRetrieveScopedCursor,
	}

	err = adapter.Close()
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to close database")
	require.ErrorIs(t, err, dbCloseErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func newMockAdapter(t *testing.T) (*Adapter, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	adapter := &Adapter{
		db:                       db,
		stmtSaveEvent:            mustPrepareStmt(t, db, mock, querySaveEvent),
		stmtRetrieveEvents:       mustPrepareStmt(t, db, mock, queryRetrieveEventsAfter),
		stmtRetrieveEventsCursor: mustPrepareStmt(t, db, mock, queryRetrieveEventsAfterCursor),
		stmtRetrieveScopedCursor: mustPrepareStmt(t, db, mock, queryRetrieveScopedEventsAfterCursor),
	}

	return adapter, mock, db
}

func mustPrepareStmt(t *testing.T, db *sql.DB, mock sqlmock.Sqlmock, query string) *sql.Stmt {
	t.Helper()

	mock.ExpectPrepare(regexp.QuoteMeta(query))
	stmt, err := db.Prepare(query)
	require.NoError(t, err)

	return stmt
}

func eventRowColumns() []string {
	return []string{
		"id",
		"tenant_id",
		"principal_id",
		"type",
		"schema_version",
		"occurred_at",
		"ingested_at",
		"metadata",
		"data",
		"ingest_seq",
	}
}
