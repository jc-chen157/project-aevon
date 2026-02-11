package postgres

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/aevon-lab/project-aevon/internal/core/partition"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestPreAggregateAdapter_FlushSkipsStaleCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewPreAggregateAdapter(db)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT checkpoint_cursor
		FROM sweep_checkpoints
		WHERE bucket_size = $1
		FOR UPDATE
	`)).WithArgs("1m").WillReturnRows(sqlmock.NewRows([]string{"checkpoint_cursor"}).AddRow(int64(100)))
	mock.ExpectRollback()

	err = adapter.Flush(context.Background(), map[aggregation.AggregateKey]aggregation.AggregateState{}, 100, "1m")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreAggregateAdapter_FlushIncludesBucketSize(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewPreAggregateAdapter(db)
	now := time.Now().UTC().Truncate(time.Second)

	key := aggregation.AggregateKey{
		PartitionID: 0,
		TenantID:    "tenant-1",
		PrincipalID: "user-1",
		RuleName:    "count_requests",
		BucketSize:  "1m",
		WindowStart: now.Truncate(time.Minute),
	}

	state := aggregation.AggregateState{
		Operator:        aggregation.OpCount,
		Value:           decimal.NewFromInt(3),
		EventCount:      3,
		LastEventID:     "evt-3",
		RuleFingerprint: "fp-1",
		UpdatedAt:       now,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT checkpoint_cursor
		FROM sweep_checkpoints
		WHERE bucket_size = $1
		FOR UPDATE
	`)).WithArgs("1m").WillReturnRows(sqlmock.NewRows([]string{"checkpoint_cursor"}).AddRow(int64(10)))

	mock.ExpectPrepare(regexp.QuoteMeta(`
		INSERT INTO pre_aggregates (
			partition_id, tenant_id, principal_id, rule_name, rule_fingerprint,
			bucket_size, window_start, operator, value, event_count, last_event_id, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (partition_id, tenant_id, principal_id, rule_name, bucket_size, window_start)
		DO UPDATE SET
			value = CASE EXCLUDED.operator
				WHEN 'count' THEN pre_aggregates.value + EXCLUDED.value
				WHEN 'sum' THEN pre_aggregates.value + EXCLUDED.value
				WHEN 'min' THEN LEAST(pre_aggregates.value, EXCLUDED.value)
				WHEN 'max' THEN GREATEST(pre_aggregates.value, EXCLUDED.value)
				ELSE EXCLUDED.value
			END,
			event_count      = pre_aggregates.event_count + EXCLUDED.event_count,
			last_event_id    = EXCLUDED.last_event_id,
			rule_fingerprint = EXCLUDED.rule_fingerprint,
			updated_at       = EXCLUDED.updated_at
	`)).ExpectExec().WithArgs(
		key.PartitionID,
		key.TenantID,
		key.PrincipalID,
		key.RuleName,
		state.RuleFingerprint,
		key.BucketSize,
		key.WindowStart,
		state.Operator,
		state.Value,
		state.EventCount,
		state.LastEventID,
		state.UpdatedAt,
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(regexp.QuoteMeta(`
		UPDATE sweep_checkpoints
		SET checkpoint_cursor = $1, updated_at = $2
		WHERE bucket_size = $3
	`)).WithArgs(int64(11), sqlmock.AnyArg(), "1m").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = adapter.Flush(context.Background(), map[aggregation.AggregateKey]aggregation.AggregateState{key: state}, 11, "1m")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreAggregateAdapter_FlushRejectsMixedBucketSizes(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewPreAggregateAdapter(db)
	now := time.Now().UTC().Truncate(time.Second)

	key := aggregation.AggregateKey{
		PartitionID: 0,
		TenantID:    "tenant-1",
		PrincipalID: "user-1",
		RuleName:    "count_requests",
		BucketSize:  "10m",
		WindowStart: now.Truncate(time.Minute),
	}

	state := aggregation.AggregateState{
		Operator:        aggregation.OpCount,
		Value:           decimal.NewFromInt(1),
		EventCount:      1,
		LastEventID:     "evt-1",
		RuleFingerprint: "fp-1",
		UpdatedAt:       now,
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(querySelectCheckpointForUpdate)).
		WithArgs("1m").
		WillReturnRows(sqlmock.NewRows([]string{"checkpoint_cursor"}).AddRow(int64(0)))
	mock.ExpectPrepare(regexp.QuoteMeta(queryUpsertPreAggregate))
	mock.ExpectRollback()

	err = adapter.Flush(
		context.Background(),
		map[aggregation.AggregateKey]aggregation.AggregateState{key: state},
		1,
		"1m",
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "aggregate bucket mismatch")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreAggregateAdapter_QueryRangeUsesPartitionAndBucket(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewPreAggregateAdapter(db)

	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	tenantID := "tenant-1"
	principalID := "user-1"
	ruleName := "count_requests"

	rows := sqlmock.NewRows([]string{
		"window_start",
		"operator",
		"value",
		"event_count",
		"last_event_id",
		"rule_fingerprint",
		"updated_at",
	}).AddRow(start, aggregation.OpCount, "3", int64(3), "evt-3", "fp-1", start.Add(time.Minute))

	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT
			window_start,
			operator,
			value,
			event_count,
			last_event_id,
			rule_fingerprint,
			updated_at
		FROM pre_aggregates
		WHERE partition_id = $1
		  AND tenant_id = $2
		  AND principal_id = $3
		  AND rule_name = $4
		  AND bucket_size = $5
		  AND window_start >= $6
		  AND window_start < $7
		ORDER BY window_start ASC
	`)).WithArgs(
		partition.For(tenantID),
		tenantID,
		principalID,
		ruleName,
		"1m",
		start,
		end,
	).WillReturnRows(rows)

	result, err := adapter.QueryRange(context.Background(), tenantID, principalID, ruleName, "1m", start, end)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "3", result[0].Value.String())
	require.Equal(t, int64(3), result[0].EventCount)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreAggregateAdapter_QueryRangeDefaultsBucketSize(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewPreAggregateAdapter(db)

	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	tenantID := "tenant-1"
	principalID := "user-1"
	ruleName := "count_requests"

	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT
			window_start,
			operator,
			value,
			event_count,
			last_event_id,
			rule_fingerprint,
			updated_at
		FROM pre_aggregates
		WHERE partition_id = $1
		  AND tenant_id = $2
		  AND principal_id = $3
		  AND rule_name = $4
		  AND bucket_size = $5
		  AND window_start >= $6
		  AND window_start < $7
		ORDER BY window_start ASC
	`)).WithArgs(
		partition.For(tenantID),
		tenantID,
		principalID,
		ruleName,
		"1m",
		start,
		end,
	).WillReturnRows(sqlmock.NewRows([]string{
		"window_start",
		"operator",
		"value",
		"event_count",
		"last_event_id",
		"rule_fingerprint",
		"updated_at",
	}))

	result, err := adapter.QueryRange(context.Background(), tenantID, principalID, ruleName, "", start, end)
	require.NoError(t, err)
	require.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreAggregateAdapter_QueryRangeWithCheckpoint(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewPreAggregateAdapter(db)

	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	tenantID := "tenant-1"
	principalID := "user-1"
	ruleName := "sum_bytes"

	mock.ExpectQuery("WITH checkpoint AS").WithArgs(
		partition.For(tenantID),
		tenantID,
		principalID,
		ruleName,
		"10m",
		start,
		end,
	).WillReturnRows(sqlmock.NewRows([]string{
		"checkpoint_cursor",
		"window_start",
		"operator",
		"value",
		"event_count",
		"last_event_id",
		"rule_fingerprint",
		"updated_at",
	}).AddRow(
		int64(120),
		start,
		aggregation.OpSum,
		"8",
		int64(2),
		"evt-2",
		"fp-1",
		start.Add(10*time.Minute),
	))

	result, checkpoint, err := adapter.QueryRangeWithCheckpoint(
		context.Background(),
		tenantID,
		principalID,
		ruleName,
		"10m",
		start,
		end,
	)
	require.NoError(t, err)
	require.Equal(t, int64(120), checkpoint)
	require.Len(t, result, 1)
	require.Equal(t, "8", result[0].Value.String())
	require.Equal(t, aggregation.OpSum, result[0].Operator)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPreAggregateAdapter_QueryRangeWithCheckpoint_EmptyRange(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	adapter := NewPreAggregateAdapter(db)

	start := time.Date(2026, 2, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	tenantID := "tenant-1"
	principalID := "user-1"
	ruleName := "count_requests"

	mock.ExpectQuery("WITH checkpoint AS").WithArgs(
		partition.For(tenantID),
		tenantID,
		principalID,
		ruleName,
		"1m",
		start,
		end,
	).WillReturnRows(sqlmock.NewRows([]string{
		"checkpoint_cursor",
		"window_start",
		"operator",
		"value",
		"event_count",
		"last_event_id",
		"rule_fingerprint",
		"updated_at",
	}).AddRow(
		int64(777),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	))

	result, checkpoint, err := adapter.QueryRangeWithCheckpoint(
		context.Background(),
		tenantID,
		principalID,
		ruleName,
		"1m",
		start,
		end,
	)
	require.NoError(t, err)
	require.Equal(t, int64(777), checkpoint)
	require.Empty(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}
