{{
    config(
        materialized='view',
        tags=['staging', 'payments']
    )
}}

with source as (
    select * from {{ source('payments', 'CHARGES') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        charge_id,
        customer_id,
        amount,
        currency,
        status,
        payment_method,
        description,
        created_at,
        captured,
        receipt_email
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
