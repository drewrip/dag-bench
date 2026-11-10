{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'TSHM') }}
),

deduped as (
    select
        shipping_method_id,
        shipping_method_code,
        shipping_method_name,
        carrier_id,
        estimated_days_min,
        estimated_days_max,
        is_express,
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
            row_number() over (partition by shipping_method_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where shipping_method_id is not null
    )
    where _rn = 1
)

select * from deduped
