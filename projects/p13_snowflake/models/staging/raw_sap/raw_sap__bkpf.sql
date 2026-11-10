{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'BKPF') }}
),

deduped as (
    select
        transaction_id,
        transaction_number,
        account_id,
        period_id,
        transaction_date,
        debit_amount,
        credit_amount,
        description,
        reference_type,
        reference_id,
        created_by,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by transaction_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where transaction_id is not null
    )
    where _rn = 1
)

select * from deduped
