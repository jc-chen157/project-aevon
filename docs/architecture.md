# Aevon Architecture

## What Aevon is

Aevon is an event-sourced state management platform.

Usage is the primary use case today (metering, quotas, limits), but the core model is broader: ingest immutable events,
then query derived state consistently over time.

Aevon is intentionally not:

- a billing product
- a policy engine
- a general analytics warehouse

It is the state layer that those systems can depend on.

## Product focus

### Problems we are solving

Teams usually hit the same failure modes as they scale:

- no reliable answer for "what state was true at time T"
- usage logic split across services, SQL jobs, and billing tools
- late events causing state drift and hard-to-debug mismatches
- vendor lock-in where billing model leaks into product behavior

Aevon provides a single, auditable state pipeline to address these.

### MVP scope (current)

In scope:

- ingest events: `POST /v1/events`
- query derived state: `GET /v1/state/{tenant_id}/{principal_id}`
- multi-tenant isolation by `tenant_id`
- deterministic aggregation over append-only events

Out of scope for core MVP:

- entitlements API
- snapshot APIs
- custom aggregation window sizes (fixed to `1m`)

## Engineering focus

### Runtime model

Aevon runs as a single Go binary with concurrent services:

- Ingestion API (write path)
- Aggregation scheduler (background pre-compute)
- Projection API (read path)

All services use PostgreSQL as the durable store.

### High-level data flow

```mermaid
flowchart LR
    C[Client] -->|POST /v1/events| I[Ingestion API]
    I --> E[(events)]

    E -->|scheduled sweep| A[Aggregation Job]
    A --> P[(pre_aggregates)]
    A --> S[(sweep_checkpoints)]

    C -->|GET /v1/state/{tenant}/{principal}| R[Projection API]
    P --> R
    S --> R
    E -->|raw tail after checkpoint| R
    R -->|aggregate response| C
```

### CQRS and consistency approach

- Write path appends immutable events and enforces idempotency.
- Aggregation path periodically materializes durable pre-aggregates.
- Read path uses a hybrid strategy:
    - read durable pre-aggregates
    - merge raw events after checkpoint

This gives near-real-time reads without requiring a fully stateful streaming system.

### Storage model (MVP)

- `events`: append-only event log (source of truth)
- `pre_aggregates`: materialized aggregate buckets
- `sweep_checkpoints`: durable cursor for aggregation progress

### Operational defaults (MVP)

- fixed aggregation bucket: `1m`
- scheduler interval from config (`aggregation.cron_interval`, default `2m`)
- rule config loaded from filesystem at startup (`aggregation.config_dir`)

## References

- `docs/aggregation-evolution.md`: why current aggregation design was chosen and when to revisit it
