{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'PROMO') }}
),

deduped as (
    select
        promotion_id,
        promotion_code,
        promotion_name,
        promotion_type,
        discount_type,
        discount_value,
        min_purchase,
        max_discount,
        start_date,
        end_date,
        is_active,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by promotion_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where promotion_id is not null
    )
    where _rn = 1
)

select * from deduped
