select
    {{ sha256_key([
        'source_system_id',
        'delivery_point_assignment_id',
        'cast(source_revision as varchar)'
    ]) }} as delivery_point_revision_key,
    history.*
from {{ ref('int_delivery_point_assignments_knowledge_history') }} as history
