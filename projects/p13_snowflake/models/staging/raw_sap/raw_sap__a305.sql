{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'A305') }}
),

deduped as (
    select
        price_id,
        variant_id,
        price_type,
        currency_code,
        price_amount,
        compare_at_price,
        cost_price,
        min_qty,
        max_qty,
        customer_tier_id,
        channel_id,
        effective_from,
        effective_to,
        is_active,
        created_at,
        updated_at,
        created_by,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by price_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where price_id is not null
    )
    where _rn = 1
)

select * from deduped
