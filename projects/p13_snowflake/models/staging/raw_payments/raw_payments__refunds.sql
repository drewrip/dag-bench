{{
    config(
        materialized='view',
        tags=['staging', 'payments']
    )
}}

with source as (
    select * from {{ source('payments', 'REFUNDS') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        refund_id,
        charge_id,
        amount,
        currency,
        status,
        reason,
        created_at
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
