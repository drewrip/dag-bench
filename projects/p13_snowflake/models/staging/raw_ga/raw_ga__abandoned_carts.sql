{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'ABANDONED_CARTS') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        cart_id,
        session_id,
        customer_id,
        abandoned_at,
        cart_value,
        item_count,
        last_page_url,
        recovery_email_sent,
        recovered
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
