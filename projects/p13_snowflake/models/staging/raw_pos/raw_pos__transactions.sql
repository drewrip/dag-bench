{{
    config(
        materialized='view',
        tags=['staging', 'pos']
    )
}}

with source as (
    select * from {{ source('pos', 'TRANSACTIONS') }}
),

deduped as (
    select
        order_id,
        order_number,
        customer_id,
        order_type,
        order_source,
        channel_id,
        currency_code,
        exchange_rate,
        billing_address_id,
        shipping_address_id,
        subtotal,
        discount_total,
        shipping_total,
        tax_total,
        grand_total,
        status,
        payment_status,
        fulfillment_status,
        ordered_at,
        shipped_at,
        delivered_at,
        cancelled_at,
        ip_address,
        user_agent,
        notes,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by order_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where order_id is not null
    )
    where _rn = 1
)

select * from deduped
