{{
    config(
        materialized='view',
        tags=['staging', 'wms']
    )
}}

with source as (
    select * from {{ source('wms', 'CYCLE_COUNTS') }}
),

deduped as (
    select
        count_id,
        count_number,
        warehouse_id,
        count_type,
        status,
        scheduled_date,
        total_locations,
        total_skus,
        total_units_counted,
        total_variance_units,
        total_variance_value,
        created_by,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by count_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where count_id is not null
    )
    where _rn = 1
)

select * from deduped
