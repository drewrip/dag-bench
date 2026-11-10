{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'BSET') }}
),

deduped as (
    select
        tax_transaction_id,
        order_id,
        invoice_id,
        tax_rate_id,
        taxable_amount,
        tax_amount,
        tax_date,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by tax_transaction_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where tax_transaction_id is not null
    )
    where _rn = 1
)

select * from deduped
