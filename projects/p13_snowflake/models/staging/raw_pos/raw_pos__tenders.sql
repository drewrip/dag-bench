{{
    config(
        materialized='view',
        tags=['staging', 'pos']
    )
}}

with source as (
    select * from {{ source('pos', 'TENDERS') }}
),

deduped as (
    select
        payment_id,
        order_id,
        payment_method_id,
        payment_method,
        amount,
        currency_code,
        status,
        transaction_id,
        authorization_code,
        card_last_four,
        card_type,
        processed_at,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by payment_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where payment_id is not null
    )
    where _rn = 1
)

select * from deduped
