{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'WISHLISTS') }}
),

deduped as (
    select
        wishlist_item_id,
        wishlist_id,
        variant_id,
        added_at,
        notes,
        priority,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by wishlist_item_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where wishlist_item_id is not null
    )
    where _rn = 1
)

select * from deduped
