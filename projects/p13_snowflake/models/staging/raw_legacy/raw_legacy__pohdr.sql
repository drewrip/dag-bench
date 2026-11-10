{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'POHDR') }}
),

deduped as (
    select
        po_id,
        po_number,
        supplier_id,
        warehouse_id,
        status,
        total_amount,
        currency_code,
        expected_date,
        ordered_at,
        created_by,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by po_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where po_id is not null
    )
    where _rn = 1
)

select * from deduped
