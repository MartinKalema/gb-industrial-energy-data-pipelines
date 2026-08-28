select
    {{ sha256_key([
        'source_system_id',
        'customer_version_id',
        'cast(source_revision as varchar)'
    ]) }} as customer_revision_key,
    history.*
from {{ ref('int_customer_versions_knowledge_history') }} as history
