{{ record_high_knowledge_history(
    ref('stg_validated__revenue_meter_reading'),
    [
        'source_system_id',
        'meter_natural_id',
        'register_natural_id',
        'reading_at_utc'
    ]
) }}
