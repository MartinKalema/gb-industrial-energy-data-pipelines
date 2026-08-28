CREATE SCHEMA IF NOT EXISTS r2.industrial_energy_smoke;

CREATE TABLE IF NOT EXISTS r2.industrial_energy_smoke.structured_stream_roundtrip (
    smoke_run_id VARCHAR,
    event_id VARCHAR,
    written_by VARCHAR,
    sequence_number BIGINT,
    note VARCHAR
)
WITH (
    format = 'PARQUET',
    format_version = 2
);
