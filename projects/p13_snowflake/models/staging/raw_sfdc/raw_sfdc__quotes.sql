{{
    config(
        materialized='view',
        tags=['staging', 'sfdc']
    )
}}

with source as (
    select * from {{ source('sfdc', 'QUOTES') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        quote_id,
        opportunity_id,
        account_id,
        quote_number,
        status,
        subtotal,
        discount,
        total,
        valid_until,
        created_date
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
