-- Consolidated baseline migration for Aevon
-- Combines migrations 001-010 into a single clean schema
-- Use this for fresh deployments only
-- For existing databases with incremental migrations, use migrations/ directory
--
-- Migration: 001_create_schema
-- Date: 2026-02-08

-- =============================================================================
-- Events Table - Event sourcing store with strict total ordering
-- =============================================================================

CREATE TABLE IF NOT EXISTS events
(
    -- Composite primary key: (tenant_id, principal_id, id)
    -- Ensures idempotency per (tenant, principal) pair
    id
    TEXT
    NOT
    NULL,
    tenant_id
    TEXT
    NOT
    NULL,
    principal_id
    TEXT
    NOT
    NULL,

    -- Event metadata
    type
    TEXT
    NOT
    NULL,
    schema_version
    INTEGER
    NOT
    NULL,
    occurred_at
    TIMESTAMPTZ
    NOT
    NULL,
    ingested_at
    TIMESTAMPTZ
    NOT
    NULL,

    -- Flexible JSON payloads
    metadata
    JSONB,
    data
    JSONB
    NOT
    NULL,

    -- Monotonic sequence for cursor-based pagination (strict total order)
    ingest_seq
    BIGSERIAL
    NOT
    NULL,

    -- Composite primary key for multi-tenant + principal idempotency
    PRIMARY
    KEY
(
    tenant_id,
    principal_id,
    id
)
    );

-- Cursor pagination index (UNIQUE enforces strict ordering)
-- Used by batch aggregation to fetch events after checkpoint
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_ingest_seq ON events (ingest_seq);

-- Projection hybrid read path index
-- Supports: SELECT ... WHERE tenant_id=? AND principal_id=? AND type=? AND ingest_seq > cursor
CREATE INDEX IF NOT EXISTS idx_events_projection_tail
    ON events (tenant_id, principal_id, type, ingest_seq);

-- Table and column documentation
COMMENT
ON TABLE events IS
    'Event store for event sourcing. Composite key (tenant_id, principal_id, id) ensures idempotency per tenant and principal.';

COMMENT
ON COLUMN events.id IS
    'Client-provided unique event ID (unique per tenant+principal pair)';

COMMENT
ON COLUMN events.tenant_id IS
    'Tenant isolation key. Defaults to "default" for self-hosted deployments.';

COMMENT
ON COLUMN events.principal_id IS
    'Principal/actor identifier (user, account, apikey). Primary dimension for usage aggregation.';

COMMENT
ON COLUMN events.occurred_at IS
    'When event happened (client clock, business timestamp)';

COMMENT
ON COLUMN events.ingested_at IS
    'When event was received by Aevon (server clock, audit timestamp)';

COMMENT
ON COLUMN events.ingest_seq IS
    'Monotonic sequence for cursor-based pagination. Provides strict total order for crash-safe checkpointing.';

COMMENT
ON COLUMN events.metadata IS
    'Optional context metadata (trace_id, source, region, etc.)';

COMMENT
ON COLUMN events.data IS
    'Event payload validated against schema registry (Type + SchemaVersion)';

-- =============================================================================
-- Pre-Aggregates Table - Computed usage buckets
-- =============================================================================

CREATE TABLE IF NOT EXISTS pre_aggregates
(
    partition_id
    INT
    NOT
    NULL,
    tenant_id
    TEXT
    NOT
    NULL,
    principal_id
    TEXT
    NOT
    NULL,
    rule_name
    TEXT
    NOT
    NULL,
    rule_fingerprint
    TEXT
    NOT
    NULL, -- SHA-256 of the rule definition; staleness detection
    bucket_size
    TEXT
    NOT
    NULL
    DEFAULT
    '1m', -- Aggregation granularity (1m, 10m, 1h)
    window_start
    TIMESTAMPTZ
    NOT
    NULL, -- Truncated to bucket boundary
    operator
    TEXT
    NOT
    NULL, -- count | sum | min | max
    value
    NUMERIC
    NOT
    NULL, -- Aggregate value (decimal for billing precision)
    event_count
    BIGINT
    NOT
    NULL
    DEFAULT
    0,    -- Number of events contributing to this bucket
    last_event_id
    TEXT, -- Most recent event that touched this row
    updated_at
    TIMESTAMPTZ
    NOT
    NULL,

    PRIMARY
    KEY
(
    partition_id,
    tenant_id,
    principal_id,
    rule_name,
    bucket_size,
    window_start
)
    );

-- NOTE: No additional indexes needed - PostgreSQL automatically creates an index for the primary key

COMMENT
ON TABLE pre_aggregates IS
    'Pre-computed usage buckets. One row per (partition, tenant, principal, rule, bucket_size, window).';

COMMENT
ON COLUMN pre_aggregates.partition_id IS
    'Partition key (0-255) for horizontal scaling. Always 0 in single-instance deployment.';

COMMENT
ON COLUMN pre_aggregates.bucket_size IS
    'Aggregation granularity label (e.g. 1m, 10m, 1h). Enables multi-resolution precompute.';

COMMENT
ON COLUMN pre_aggregates.rule_fingerprint IS
    'SHA-256 of the rule definition. Used for staleness detection at query time.';

COMMENT
ON COLUMN pre_aggregates.value IS
    'Aggregate value using NUMERIC for exact decimal arithmetic (critical for billing).';

COMMENT
ON COLUMN pre_aggregates.event_count IS
    'Number of events contributing to this bucket. Used for idempotency and debugging.';

-- =============================================================================
-- Sweep Checkpoints Table - Cursor tracking per bucket size
-- =============================================================================

CREATE TABLE IF NOT EXISTS sweep_checkpoints
(
    bucket_size
    TEXT
    PRIMARY
    KEY, -- Aggregation granularity (1m, 10m, 1h)
    checkpoint_cursor
    BIGINT
    NOT
    NULL
    DEFAULT
    0,   -- Last ingest_seq durably aggregated
    updated_at
    TIMESTAMPTZ
    NOT
    NULL
);

-- Insert default checkpoint for 1-minute buckets
INSERT INTO sweep_checkpoints (bucket_size, checkpoint_cursor, updated_at)
VALUES ('1m', 0, NOW()) ON CONFLICT (bucket_size) DO NOTHING;

COMMENT
ON TABLE sweep_checkpoints IS
    'Checkpoint cursor per bucket_size. Cursor N means all events <= N are durably aggregated for that bucket.';

COMMENT
ON COLUMN sweep_checkpoints.bucket_size IS
    'Aggregation bucket granularity label (e.g. 1m, 10m, 1h).';

COMMENT
ON COLUMN sweep_checkpoints.checkpoint_cursor IS
    'Last ingest_seq included in durable state for this bucket_size. Replay resumes from cursor+1.';
