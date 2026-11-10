{{
    config(
        materialized='view',
        tags=['staging', 'wms']
    )
}}

with source as (
    select * from {{ source('wms', 'ZONES') }}
),

deduped as (
    select
        zone_id,
        warehouse_id,
        zone_code,
        zone_name,
        zone_type,
        temperature_controlled,
        min_temperature,
        max_temperature,
        capacity_units,
        sort_order,
        is_active,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by zone_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where zone_id is not null
    )
    where _rn = 1
)

select * from deduped
