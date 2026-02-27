CREATE TABLE IF NOT EXISTS ingestion_log (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64),
    status VARCHAR(50) NOT NULL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_records (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    record JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_airtravel (
    month VARCHAR(10),
    year_1958 INT,
    year_1959 INT,
    year_1960 INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);