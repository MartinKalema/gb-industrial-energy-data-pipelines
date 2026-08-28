select
    context.*,

    opening.source_reading_revision_id as opening_source_reading_revision_id,
    opening.source_revision as opening_reading_source_revision,
    opening.revision_type as opening_reading_revision_type,
    opening.cumulative_value as opening_register_native_value,
    opening.native_unit as opening_register_native_unit,
    opening.published_at_utc as opening_reading_published_at_utc,
    opening.approved_at_utc as opening_reading_approved_at_utc,
    opening.pipeline_payload_sha256 as opening_reading_pipeline_payload_sha256,

    closing.source_reading_revision_id as closing_source_reading_revision_id,
    closing.source_revision as closing_reading_source_revision,
    closing.revision_type as closing_reading_revision_type,
    closing.cumulative_value as closing_register_native_value,
    closing.native_unit as closing_register_native_unit,
    closing.published_at_utc as closing_reading_published_at_utc,
    closing.approved_at_utc as closing_reading_approved_at_utc,
    closing.pipeline_payload_sha256 as closing_reading_pipeline_payload_sha256,

    case
        when context.meter_assignment_id is null then 'meter_assignment_missing'
        when opening.source_reading_revision_id is null
          or closing.source_reading_revision_id is null then 'boundary_missing'
        when opening.reading_status = 'withdrawn'
          or closing.reading_status = 'withdrawn' then 'boundary_withdrawn'
        when opening.reading_method <> 'actual'
          or closing.reading_method <> 'actual' then 'non_actual_reading'
        when opening.native_unit <> context.meter_native_unit
          or closing.native_unit <> context.meter_native_unit then 'unit_mismatch'
        when closing.cumulative_value - opening.cumulative_value < decimal '0.000000'
            then 'negative_delta'
        when closing.cumulative_value - opening.cumulative_value
             > context.maximum_plausible_30_min_change
            then 'implausible_delta'
        else 'accepted'
    end as delivery_measurement_status,

    case
        when opening.source_reading_revision_id is not null
         and opening.reading_status = 'active'
         and opening.reading_method = 'actual'
         and opening.native_unit = context.meter_native_unit
            then {{ thermal_energy_to_mwh(
                'opening.cumulative_value',
                'opening.native_unit'
            ) }}
    end as opening_register_mwh_th,
    case
        when closing.source_reading_revision_id is not null
         and closing.reading_status = 'active'
         and closing.reading_method = 'actual'
         and closing.native_unit = context.meter_native_unit
            then {{ thermal_energy_to_mwh(
                'closing.cumulative_value',
                'closing.native_unit'
            ) }}
    end as closing_register_mwh_th,
    case
        when opening.source_reading_revision_id is not null
         and closing.source_reading_revision_id is not null
         and opening.reading_status = 'active'
         and closing.reading_status = 'active'
         and opening.reading_method = 'actual'
         and closing.reading_method = 'actual'
         and opening.native_unit = context.meter_native_unit
         and closing.native_unit = context.meter_native_unit
         and closing.cumulative_value - opening.cumulative_value
                between decimal '0.000000'
                    and context.maximum_plausible_30_min_change
            then {{ thermal_energy_to_mwh(
                'closing.cumulative_value - opening.cumulative_value',
                'opening.native_unit'
            ) }}
    end as delivered_mwh_th
from {{ ref('int_delivery_interval_context') }} as context
left join {{ ref('int_current_meter_readings') }} as opening
  on context.meter_natural_id = opening.meter_natural_id
 and context.register_natural_id = opening.register_natural_id
 and context.interval_start_utc = opening.reading_at_utc
 and opening.register_type = 'thermal_energy'
left join {{ ref('int_current_meter_readings') }} as closing
  on context.meter_natural_id = closing.meter_natural_id
 and context.register_natural_id = closing.register_natural_id
 and context.interval_end_utc = closing.reading_at_utc
 and closing.register_type = 'thermal_energy'
