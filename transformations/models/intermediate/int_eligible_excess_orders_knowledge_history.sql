{{ record_high_knowledge_history(
    ref('stg_validated__approved_excess_order'),
    ['source_system_id', 'order_interval_line_id'],
    "published_at_utc < interval_start_utc and approved_at_utc < interval_start_utc"
) }}
