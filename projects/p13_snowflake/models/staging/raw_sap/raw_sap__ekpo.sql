{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'EKPO') }}
),

deduped as (
    select
        po_line_id,
        po_id,
        line_number,
        variant_id,
        sku,
        quantity_ordered,
        quantity_received,
        unit_price,
        line_total,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by po_line_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where po_line_id is not null
    )
    where _rn = 1
)

select * from deduped
