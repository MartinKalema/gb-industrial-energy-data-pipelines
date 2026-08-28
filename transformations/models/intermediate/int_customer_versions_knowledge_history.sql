{{ record_high_knowledge_history(
    ref('stg_validated__customer_master'),
    ['source_system_id', 'customer_version_id']
) }}
