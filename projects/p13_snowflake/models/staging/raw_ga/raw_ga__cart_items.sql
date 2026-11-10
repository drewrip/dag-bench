{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'CART_ITEMS') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        cart_item_id,
        cart_id,
        product_id,
        variant_id,
        quantity,
        unit_price,
        added_at
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
