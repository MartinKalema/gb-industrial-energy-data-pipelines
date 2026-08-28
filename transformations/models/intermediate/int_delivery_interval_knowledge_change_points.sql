with spine as (
    select * from {{ ref('int_delivery_interval_history_spine') }}
),

delivery_point_relationships as (
    select distinct
        source_system_id,
        delivery_point_assignment_id,
        delivery_point_natural_id,
        customer_natural_id,
        site_natural_id
    from {{ ref('stg_validated__delivery_point_assignment') }}
),

meter_relationships as (
    select distinct
        source_system_id,
        meter_assignment_id,
        delivery_point_natural_id,
        meter_natural_id,
        register_natural_id
    from {{ ref('stg_validated__revenue_meter_assignment') }}
),

contract_relationships as (
    select distinct
        source_system_id,
        contract_terms_version_id,
        delivery_point_natural_id
    from {{ ref('stg_validated__contract_terms') }}
),

relevant_points as (
    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_delivery_point_assignments_knowledge_history') }} as history
    inner join delivery_point_relationships as relationship
      on history.source_system_id = relationship.source_system_id
     and history.delivery_point_assignment_id = relationship.delivery_point_assignment_id
    inner join spine
      on relationship.delivery_point_natural_id = spine.delivery_point_natural_id

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_customer_versions_knowledge_history') }} as history
    inner join delivery_point_relationships as relationship
      on history.customer_natural_id = relationship.customer_natural_id
    inner join spine
      on relationship.delivery_point_natural_id = spine.delivery_point_natural_id

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_site_versions_knowledge_history') }} as history
    inner join delivery_point_relationships as relationship
      on history.site_natural_id = relationship.site_natural_id
    inner join spine
      on relationship.delivery_point_natural_id = spine.delivery_point_natural_id

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_meter_assignments_knowledge_history') }} as history
    inner join meter_relationships as relationship
      on history.source_system_id = relationship.source_system_id
     and history.meter_assignment_id = relationship.meter_assignment_id
    inner join spine
      on relationship.delivery_point_natural_id = spine.delivery_point_natural_id

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_contract_terms_knowledge_history') }} as history
    inner join contract_relationships as relationship
      on history.source_system_id = relationship.source_system_id
     and history.contract_terms_version_id = relationship.contract_terms_version_id
    inner join spine
      on relationship.delivery_point_natural_id = spine.delivery_point_natural_id

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_commitments_knowledge_history') }} as history
    inner join spine
      on history.delivery_point_natural_id = spine.delivery_point_natural_id
     and history.interval_start_utc = spine.interval_start_utc

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_eligible_excess_orders_knowledge_history') }} as history
    inner join spine
      on history.delivery_point_natural_id = spine.delivery_point_natural_id
     and history.interval_start_utc = spine.interval_start_utc

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_meter_readings_knowledge_history') }} as history
    inner join meter_relationships as relationship
      on history.meter_natural_id = relationship.meter_natural_id
     and history.register_natural_id = relationship.register_natural_id
    inner join spine
      on relationship.delivery_point_natural_id = spine.delivery_point_natural_id
     and history.reading_at_utc in (spine.interval_start_utc, spine.interval_end_utc)

    union

    select spine.delivery_interval_key, history.known_from_utc
    from {{ ref('int_capacity_assessments_knowledge_history') }} as history
    inner join spine
      on history.delivery_point_natural_id = spine.delivery_point_natural_id
     and history.interval_start_utc = spine.interval_start_utc
)

select
    spine.*,
    relevant_points.known_from_utc as knowledge_point_utc
from spine
inner join relevant_points
  on spine.delivery_interval_key = relevant_points.delivery_interval_key
where relevant_points.known_from_utc is not null
