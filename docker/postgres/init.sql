-- =================================================================
-- Banking Data Engineering Platform — PostgreSQL Initialization
-- =================================================================
-- This script runs once when the PostgreSQL container starts.
-- It creates:
--   1. The banking_dw database
--   2. The banking_user with appropriate permissions
--   3. The four schemas: staging, warehouse, analytics, quality
--   4. Extensions needed (uuid-ossp for UUID generation)
--
-- Why four schemas?
--   staging   → Raw data landed directly from pipeline, minimal transforms
--   warehouse → Cleaned star-schema dimensional data
--   analytics → Aggregated tables for BI tools / dashboards
--   quality   → Data quality results, pipeline run metadata, rejections
-- =================================================================

-- Create banking database (run as postgres superuser)
SELECT 'CREATE DATABASE banking_dw'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'banking_dw')
\gexec

-- Create application user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'banking_user') THEN
        CREATE USER banking_user WITH PASSWORD 'banking_password_dev';
    END IF;
END
$$;

-- Grant connect
GRANT CONNECT ON DATABASE banking_dw TO banking_user;

-- Switch to banking_dw database for schema setup
\connect banking_dw

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---- Create schemas ----

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS quality;

-- Grant schema permissions to banking_user
GRANT USAGE ON SCHEMA staging TO banking_user;
GRANT USAGE ON SCHEMA warehouse TO banking_user;
GRANT USAGE ON SCHEMA analytics TO banking_user;
GRANT USAGE ON SCHEMA quality TO banking_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO banking_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO banking_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO banking_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA quality TO banking_user;

-- Ensure future tables are also accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO banking_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT ALL ON TABLES TO banking_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO banking_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA quality GRANT ALL ON TABLES TO banking_user;

-- Also grant sequence permissions (for SERIAL/IDENTITY columns)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA staging TO banking_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA warehouse TO banking_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA quality TO banking_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT USAGE ON SEQUENCES TO banking_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT USAGE ON SEQUENCES TO banking_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA quality GRANT USAGE ON SEQUENCES TO banking_user;

-- ================================================================
-- QUALITY SCHEMA TABLES
-- These are created at init because pipeline metadata is needed
-- from the very first pipeline run.
-- ================================================================

-- Pipeline run tracking
CREATE TABLE IF NOT EXISTS quality.pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name   VARCHAR(100) NOT NULL,
    dag_id          VARCHAR(100),
    task_id         VARCHAR(100),
    start_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time        TIMESTAMP WITH TIME ZONE,
    status          VARCHAR(20) NOT NULL DEFAULT 'RUNNING',  -- RUNNING | SUCCESS | FAILED | PARTIAL
    records_read    BIGINT DEFAULT 0,
    records_processed BIGINT DEFAULT 0,
    records_rejected BIGINT DEFAULT 0,
    records_loaded  BIGINT DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Data quality check results
CREATE TABLE IF NOT EXISTS quality.quality_results (
    result_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id          UUID REFERENCES quality.pipeline_runs(run_id),
    dataset         VARCHAR(50) NOT NULL,
    check_name      VARCHAR(100) NOT NULL,
    check_type      VARCHAR(50) NOT NULL,  -- COMPLETENESS | UNIQUENESS | VALIDITY | REFERENTIAL | RANGE | TIMESTAMP
    total_records   BIGINT NOT NULL DEFAULT 0,
    passed_records  BIGINT NOT NULL DEFAULT 0,
    failed_records  BIGINT NOT NULL DEFAULT 0,
    pass_rate       DECIMAL(5,2),
    status          VARCHAR(20) NOT NULL,  -- PASS | FAIL | WARN
    details         JSONB,
    checked_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Rejected record tracking
CREATE TABLE IF NOT EXISTS quality.rejected_records (
    rejection_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id          UUID REFERENCES quality.pipeline_runs(run_id),
    dataset         VARCHAR(50) NOT NULL,
    record_id       VARCHAR(100),
    failure_reason  VARCHAR(100) NOT NULL,
    failed_rule     VARCHAR(100) NOT NULL,
    raw_data        JSONB,
    rejected_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Watermarks for incremental processing
-- Why: Stores the last successfully processed timestamp per dataset
-- so the next run only processes NEW records.
CREATE TABLE IF NOT EXISTS quality.pipeline_watermarks (
    watermark_id    SERIAL PRIMARY KEY,
    dataset         VARCHAR(50) NOT NULL UNIQUE,
    last_processed  TIMESTAMP WITH TIME ZONE NOT NULL,
    last_run_id     UUID,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Reconciliation results
CREATE TABLE IF NOT EXISTS quality.reconciliation_results (
    reconciliation_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id              UUID REFERENCES quality.pipeline_runs(run_id),
    dataset             VARCHAR(50) NOT NULL,
    source_count        BIGINT,
    target_count        BIGINT,
    source_amount       DECIMAL(20,2),
    target_amount       DECIMAL(20,2),
    count_difference    BIGINT,
    amount_difference   DECIMAL(20,2),
    count_status        VARCHAR(10),  -- PASS | FAIL
    amount_status       VARCHAR(10),  -- PASS | FAIL
    overall_status      VARCHAR(10),  -- PASS | FAIL
    run_timestamp       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Useful indexes on quality tables
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON quality.pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start_time ON quality.pipeline_runs(start_time);
CREATE INDEX IF NOT EXISTS idx_quality_results_run_id ON quality.quality_results(run_id);
CREATE INDEX IF NOT EXISTS idx_quality_results_dataset ON quality.quality_results(dataset);
CREATE INDEX IF NOT EXISTS idx_rejected_records_run_id ON quality.rejected_records(run_id);
CREATE INDEX IF NOT EXISTS idx_rejected_records_dataset ON quality.rejected_records(dataset);
CREATE INDEX IF NOT EXISTS idx_rejected_records_reason ON quality.rejected_records(failure_reason);

-- Confirmation message
DO $$ BEGIN
    RAISE NOTICE 'Banking DW initialized: schemas=staging,warehouse,analytics,quality | extensions=uuid-ossp';
END $$;
