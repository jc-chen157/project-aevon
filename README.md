# Aevon

Aevon is an event-sourced state management platform for economic and operational state.
Usage is the primary use case today, but the core model is broader: you write immutable events and query derived state
over time.

The goal is simple: give you a reliable source of state truth that is auditable, replayable, and fast to query.

## Philosophy

- Aevon is a state engine, not a policy engine. It computes state; your application decides what to do with it.
- Aevon is also not a billing suite. It can power billing workflows, but it is designed to remain independent from any
  single billing ecosystem.
- Events are the source of truth. Derived state can be rebuilt and verified from history.
- Usage is first-class because most teams feel state pain there first (quotas, limits, metering, attribution), but the
  architecture is intentionally state-first.

## Product focus: What problem this solves

Teams usually start usage tracking inside app code and billing workflows, then outgrow it. Common issues:

- no trusted answer for "what state was true at time T"
- product decisions (quota, throttling, limits) depend on scattered logic
- late or out-of-order events create drift between systems
- billing vendors become accidental sources of product state

Aevon addresses this by making state derivation a first-class platform concern:

- one place to ingest immutable events
- one consistent way to compute and query derived state
- auditable history and replay when needed
- usage handled as a primary use case, without locking your product model to a billing tool

## Product focus: MVP scope (current)

What you can ship with today:

- ingest events through `POST /v1/events`
- query derived state through `GET /v1/state/{principal_id}`
- power core product workflows like usage counters, quota checks, and limit-aware UX
- get deterministic, replayable state from append-only history

Intentionally out of scope for core MVP:

- entitlements APIs
- snapshot management APIs
- custom aggregation window sizes beyond fixed `1m`

## Engineering focus: Architecture

```
Client -> POST /v1/events
          -> events table (append-only)
          -> scheduler sweeps events every interval
          -> pre_aggregates + sweep_checkpoints

Client -> GET /v1/state/{principal_id}
          -> pre-aggregates (durable)
          -> + raw tail since checkpoint
          -> merged response
```

Aevon uses a CQRS/event-sourced core with three execution paths:

- Write path: HTTP ingestion validates envelope/schema, enforces idempotency, and appends to `events` with monotonic
  `ingest_seq`.
- Aggregation path: a scheduler reads events in order, applies configured rules, and flushes `pre_aggregates` plus
  checkpoint cursor.
- Read path: projection queries durable aggregates, then merges any raw tail events after checkpoint to keep results
  fresh.

Engineering defaults in MVP:

- fixed bucket size: `1m`
- rule loading from filesystem at startup
- PostgreSQL as source of durability for events, aggregates, and checkpoints
- graceful shutdown through context cancellation across server and schedulers

## Quick start

### Prerequisites

- Go 1.24+
- Docker + Docker Compose

### 1. Start PostgreSQL and run migrations

```bash
make db-up
make db-migrate
```

### 2. Add at least one aggregation rule

By default, Aevon loads rules from `./config/aggregations`.
If this directory is empty or missing, state queries will return `unknown rule`.

Create the rule directory and file:

```bash
mkdir -p config/aggregations
```

Then create `config/aggregations/count_api_requests.yaml`:

```yaml
name: "count_api_requests"
source_event: "api.request"
operator: "count"
```

Notes:

- `window_size` customization is disabled in MVP. Buckets are always `1m`.
- Supported operators: `count`, `sum`, `min`, `max`.

### 3. Run the service

```bash
make run
```

Server starts on `http://localhost:8080` by default.

Health check:

```bash
curl http://localhost:8080/health
```

### 4. Ingest an event

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt-001",
    "principal_id": "user_123",
    "type": "api.request",
    "schema_version": 0,
    "occurred_at": "2026-02-11T10:30:00Z",
    "data": {}
  }'
```

Expected response:

```json
{
  "status": "accepted"
}
```

### 5. Query aggregate state

```bash
curl "http://localhost:8080/v1/state/user_123?rule=count_api_requests&start=2026-02-11T10:00:00Z&end=2026-02-11T11:00:00Z&granularity=total"
```

Example response:

```json
{
  "principal_id": "user_123",
  "rule": "count_api_requests",
  "operator": "count",
  "start": "2026-02-11T10:00:00Z",
  "end": "2026-02-11T11:00:00Z",
  "granularity": "total",
  "data_through": "2026-02-11T11:00:00Z",
  "staleness_seconds": 0,
  "values": [
    {
      "window_start": "2026-02-11T10:00:00Z",
      "window_end": "2026-02-11T11:00:00Z",
      "value": "1",
      "event_count": 1
    }
  ]
}
```

## API reference (core MVP)

### POST /v1/events

Ingests one immutable event.

Required fields:

- `id` (string)
- `principal_id` (string)
- `type` (string)
- `occurred_at` (RFC3339 timestamp)
- `data` (object)

Optional fields:

- `schema_version` (int, `0` skips schema validation)
- `metadata` (map)

Responses:

- `202 Accepted` on success
- `409 Conflict` for duplicate event ID
- `400 Bad Request` for validation/schema errors

Backward-compatible alias: `POST /v1/ingest`

### GET /v1/state/{principal_id}

Queries aggregated values for principal.

Query params:

- `rule` (required)
- `start` (required, RFC3339)
- `end` (required, RFC3339)
- `granularity` (optional): `total`, `1m`, `1h`, `1d` (default: `total`)

Responses:

- `200 OK` with aggregate values
- `400 Bad Request` for invalid query or unknown rule

Backward-compatible alias: `GET /v1/aggregates/{principal_id}`

## Configuration

Default config is in `aevon.yaml`.

Important keys:

- `database.dsn`: PostgreSQL DSN
- `schema.path`: schema directory (`./schemas`)
- `aggregation.config_dir`: rule directory (`./config/aggregations`)
- `aggregation.cron_interval`: scheduler interval (example: `2m`)

## Development

Common commands:

```bash
make build
make test
make test-integration
make fmt
make vet
```

Integration tests expect PostgreSQL on:

`postgres://aevon_dev:dev_password@localhost:5432/aevon?sslmode=disable`

## Current design choices

- Event store is append-only.
- Aggregation buckets are fixed at `1m` for MVP simplicity.
- Rule loading is file-based at startup (no hot reload).
- Read path merges durable pre-aggregates with raw tail events after checkpoint.

These constraints keep the system operationally simple while we harden the core loop.

## Project layout

- `cmd/aevon`: application entrypoint
- `internal/ingestion`: write path and HTTP ingestion handler
- `internal/projection`: read path and state query handler
- `internal/aggregation`: scheduler, batch job, rule loading
- `internal/core/storage/postgres`: PostgreSQL adapters
- `internal/migrations` and `migrations`: SQL migrations
- `schemas`: event schema files

## License

Licensed under FSL-1.1. See `LICENSE`.
