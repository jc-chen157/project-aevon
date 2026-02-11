# Aggregation Evolution

This document explains why Aevon currently uses scheduled batch aggregation with a hybrid read path.

## Decision summary

Current choice:

- cron-based batch aggregation
- durable pre-aggregates in PostgreSQL
- projection merges pre-aggregates with raw tail events after checkpoint

Why this is the right default for MVP:

- materially simpler operations than stateful real-time processing
- strong auditability through append-only events
- good enough freshness for usage and quota workflows
- clear migration path if scale/latency requirements change

## How we got here

### Phase 1: event log only

Approach:

- store events
- compute aggregates directly at query time

Outcome:

- simple prototype
- query cost grows with event volume
- not suitable for sustained production load

### Phase 2: stateful real-time aggregation

Approach:

- in-memory state map
- worker loop + sweeper + recovery logic

Outcome:

- low-latency updates
- significantly higher implementation and operational complexity
- more concurrency and recovery edge cases than needed for MVP requirements

### Phase 3: scheduled batch (current)

Approach:

- scheduler drains events after checkpoint
- aggregates and flushes results durably
- projection layer bridges freshness gap with raw tail merge

Outcome:

- much lower complexity
- deterministic recovery from durable checkpoint
- predictable behavior under restart/failure

## Trade-offs we accept today

- pre-aggregate materialization is interval-based, not continuous
- projection may scan raw tail when scheduler is behind
- fixed `1m` bucket keeps implementation simple but limits flexibility

For the current product stage, these are acceptable trade-offs.

## When to revisit stateful real-time

Consider a more stateful architecture if one or more are true:

- sustained ingest rate exceeds batch comfort zone
- hard product requirement for sub-10s state visibility
- tail-scan cost materially impacts p95/p99 read latency
- operational model requires push-style near-instant reactions

If those triggers appear, we can evolve without replacing core principles:

- keep event sourcing and immutable log
- keep deterministic rule semantics
- swap aggregation execution model behind existing APIs
