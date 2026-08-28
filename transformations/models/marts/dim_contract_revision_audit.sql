select
    {{ sha256_key([
        'source_system_id',
        'contract_terms_version_id',
        'cast(source_revision as varchar)'
    ]) }} as contract_revision_key,
    history.*
from {{ ref('int_contract_terms_knowledge_history') }} as history
