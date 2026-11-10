{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'VBAP') }}
),

deduped as (
    select
        order_line_id,
        order_id,
        line_number,
        variant_id,
        product_id,
        sku,
        product_name,
        variant_name,
        quantity_ordered,
        quantity_shipped,
        quantity_returned,
        unit_price,
        discount_amount,
        tax_amount,
        line_total,
        status,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by order_line_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where order_line_id is not null
    )
    where _rn = 1
)

select * from deduped
