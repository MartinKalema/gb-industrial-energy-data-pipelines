with statuses as (
    select distinct
        delivery_measurement_status,
        commitment_status,
        capacity_status,
        contract_status,
        customer_access_status,
        sla_result_status,
        availability_result_status,
        financial_result_status,
        correction_status
    from {{ ref('fct_steam_delivery_interval') }}

    union

    select distinct
        delivery_measurement_status,
        commitment_status,
        capacity_status,
        contract_status,
        customer_access_status,
        sla_result_status,
        availability_result_status,
        financial_result_status,
        correction_status
    from {{ ref('fct_steam_delivery_interval_history') }}
)

select
    {{ sha256_key([
        'delivery_measurement_status',
        'commitment_status',
        'capacity_status',
        'contract_status',
        'customer_access_status',
        'sla_result_status',
        'availability_result_status',
        'financial_result_status',
        'correction_status'
    ]) }} as data_status_key,
    *,
    delivery_measurement_status = 'accepted' as delivery_is_accepted,
    capacity_status = 'final' as capacity_is_final,
    sla_result_status = 'final' as sla_is_final,
    availability_result_status = 'final' as availability_is_final,
    financial_result_status = 'final' as financials_are_final,
    customer_access_status = 'authorized' as customer_access_is_authorized
from statuses
