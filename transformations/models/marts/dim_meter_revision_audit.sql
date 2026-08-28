select
    {{ sha256_key([
        'source_system_id',
        'meter_assignment_id',
        'cast(source_revision as varchar)'
    ]) }} as meter_revision_key,
    history.*
from {{ ref('int_meter_assignments_knowledge_history') }} as history
