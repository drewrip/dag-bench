{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'MARA_VAR') }}
),

deduped as (
    select
        variant_id,
        product_id,
        sku,
        variant_name,
        variant_description,
        barcode,
        barcode_type,
        gtin,
        mpn,
        weight,
        weight_uom,
        length,
        width,
        height,
        dimension_uom,
        cost_price,
        compare_at_price,
        requires_shipping,
        is_taxable,
        inventory_policy,
        fulfillment_service,
        image_url,
        sort_order,
        is_default,
        is_active,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by variant_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where variant_id is not null
    )
    where _rn = 1
)

select * from deduped
