SELECT
    smoke_run_id,
    count(*) AS event_count,
    min(sequence_number) AS first_sequence,
    max(sequence_number) AS last_sequence
FROM r2.industrial_energy_smoke.structured_stream_roundtrip
GROUP BY smoke_run_id
ORDER BY smoke_run_id DESC
LIMIT 5;
