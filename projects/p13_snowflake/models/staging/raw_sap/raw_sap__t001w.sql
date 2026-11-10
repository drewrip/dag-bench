{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'T001W') }}
),

deduped as (
    select
        warehouse_id,
        warehouse_code,
        warehouse_name,
        warehouse_type,
        address_line_1,
        city,
        state_province,
        postal_code,
        country_code,
        latitude,
        longitude,
        timezone,
        phone,
        email,
        manager_name,
        square_footage,
        max_capacity_units,
        opened_date,
        priority,
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
            row_number() over (partition by warehouse_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where warehouse_id is not null
    )
    where _rn = 1
)

select * from deduped
