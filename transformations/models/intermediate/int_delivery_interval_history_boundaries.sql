select
    context.*,

    opening.source_reading_revision_id as history_opening_source_reading_revision_id,
    opening.source_revision as history_opening_reading_source_revision,
    opening.reading_status as history_opening_reading_status,
    opening.reading_method as history_opening_reading_method,
    opening.cumulative_value as history_opening_register_native_value,
    opening.native_unit as history_opening_register_native_unit,
    opening.published_at_utc as history_opening_reading_published_at_utc,
    opening.approved_at_utc as history_opening_reading_approved_at_utc,
    opening.pipeline_payload_sha256 as history_opening_reading_pipeline_payload_sha256,

    closing.source_reading_revision_id as history_closing_source_reading_revision_id,
    closing.source_revision as history_closing_reading_source_revision,
    closing.reading_status as history_closing_reading_status,
    closing.reading_method as history_closing_reading_method,
    closing.cumulative_value as history_closing_register_native_value,
    closing.native_unit as history_closing_register_native_unit,
    closing.published_at_utc as history_closing_reading_published_at_utc,
    closing.approved_at_utc as history_closing_reading_approved_at_utc,
    closing.pipeline_payload_sha256 as history_closing_reading_pipeline_payload_sha256,

    case
        when context.history_meter_assignment_id is null then 'meter_assignment_missing'
        when opening.source_reading_revision_id is null
          or closing.source_reading_revision_id is null then 'boundary_missing'
        when opening.reading_status = 'withdrawn'
          or closing.reading_status = 'withdrawn' then 'boundary_withdrawn'
        when opening.reading_method <> 'actual'
          or closing.reading_method <> 'actual' then 'non_actual_reading'
        when opening.native_unit <> context.history_meter_native_unit
          or closing.native_unit <> context.history_meter_native_unit then 'unit_mismatch'
        when closing.cumulative_value - opening.cumulative_value < decimal '0.000000'
            then 'negative_delta'
        when closing.cumulative_value - opening.cumulative_value
             > context.history_maximum_plausible_30_min_change
            then 'implausible_delta'
        else 'accepted'
    end as history_delivery_measurement_status,

    case
        when opening.reading_status = 'active'
         and opening.reading_method = 'actual'
         and opening.native_unit = context.history_meter_native_unit
            then {{ thermal_energy_to_mwh(
                'opening.cumulative_value',
                'opening.native_unit'
            ) }}
    end as history_opening_register_mwh_th,
    case
        when closing.reading_status = 'active'
         and closing.reading_method = 'actual'
         and closing.native_unit = context.history_meter_native_unit
            then {{ thermal_energy_to_mwh(
                'closing.cumulative_value',
                'closing.native_unit'
            ) }}
    end as history_closing_register_mwh_th,
    case
        when opening.reading_status = 'active'
         and closing.reading_status = 'active'
         and opening.reading_method = 'actual'
         and closing.reading_method = 'actual'
         and opening.native_unit = context.history_meter_native_unit
         and closing.native_unit = context.history_meter_native_unit
         and closing.cumulative_value - opening.cumulative_value
                between decimal '0.000000'
                    and context.history_maximum_plausible_30_min_change
            then {{ thermal_energy_to_mwh(
                'closing.cumulative_value - opening.cumulative_value',
                'opening.native_unit'
            ) }}
    end as history_delivered_mwh_th
from {{ ref('int_delivery_interval_history_context') }} as context
left join {{ ref('int_meter_readings_knowledge_history') }} as opening
  on context.history_meter_natural_id = opening.meter_natural_id
 and context.history_register_natural_id = opening.register_natural_id
 and context.interval_start_utc = opening.reading_at_utc
 and opening.known_from_utc <= context.knowledge_point_utc
 and (opening.known_to_utc is null or context.knowledge_point_utc < opening.known_to_utc)
left join {{ ref('int_meter_readings_knowledge_history') }} as closing
  on context.history_meter_natural_id = closing.meter_natural_id
 and context.history_register_natural_id = closing.register_natural_id
 and context.interval_end_utc = closing.reading_at_utc
 and closing.known_from_utc <= context.knowledge_point_utc
 and (closing.known_to_utc is null or context.knowledge_point_utc < closing.known_to_utc)
