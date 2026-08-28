select
    {{ sha256_key([
        'source_system_id',
        'site_version_id',
        'cast(source_revision as varchar)'
    ]) }} as site_revision_key,
    history.*
from {{ ref('int_site_versions_knowledge_history') }} as history
