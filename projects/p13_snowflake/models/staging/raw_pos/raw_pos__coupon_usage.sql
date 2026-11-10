{{
    config(
        materialized='view',
        tags=['staging', 'pos']
    )
}}

with source as (
    select * from {{ source('pos', 'COUPON_USAGE') }}
),

deduped as (
    select
        redemption_id,
        coupon_id,
        order_id,
        customer_id,
        discount_amount,
        redeemed_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by redemption_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where redemption_id is not null
    )
    where _rn = 1
)

select * from deduped
