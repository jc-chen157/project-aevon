-- Rollback migration 001: Drop all tables
-- Date: 2026-02-10

DROP TABLE IF EXISTS sweep_checkpoints;
DROP TABLE IF EXISTS pre_aggregates;
DROP TABLE IF EXISTS events;
