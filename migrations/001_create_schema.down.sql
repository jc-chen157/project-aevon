-- Rollback consolidated baseline migration
-- Drops all tables created by 001_create_schema.up.sql

DROP TABLE IF EXISTS sweep_checkpoints;
DROP TABLE IF EXISTS pre_aggregates;
DROP TABLE IF EXISTS events;
