-- Database Schema for Public Data Platform

-- 1. Ingestion Log Table (Audit trail for file ingestion)
CREATE TABLE IF NOT EXISTS ingestion_log (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, SKIPPED
    records_count INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Raw Records Table (JSONB Storage - Immutable Raw Layer)
CREATE TABLE IF NOT EXISTS raw_records (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    record JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Staging Table (Structured Layer)
CREATE TABLE IF NOT EXISTS stg_airtravel (
    stg_id SERIAL PRIMARY KEY,
    month VARCHAR(10),
    year_1958 INT,
    year_1959 INT,
    year_1960 INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Warehouse Layer (Star Schema)
-- Dimension Table: Time/Months
CREATE TABLE IF NOT EXISTS dim_month (
    month_id SERIAL PRIMARY KEY,
    month_name VARCHAR(10) UNIQUE NOT NULL
);

-- Fact Table: Air Travel
CREATE TABLE IF NOT EXISTS fct_air_travel (
    fact_id SERIAL PRIMARY KEY,
    month_id INTEGER REFERENCES dim_month(month_id),
    year_val INTEGER NOT NULL,
    passenger_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Pipeline Monitoring Table
CREATE TABLE IF NOT EXISTS pipeline_run_history (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds FLOAT,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED
    error_message TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_records_source ON raw_records(source_name);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_status ON ingestion_log(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_hash ON ingestion_log(file_hash);
