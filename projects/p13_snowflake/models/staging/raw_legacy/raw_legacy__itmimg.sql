{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'ITMIMG') }}
),

deduped as (
    select
        image_id,
        product_id,
        variant_id,
        image_url,
        thumbnail_url,
        alt_text,
        image_type,
        sort_order,
        width,
        height,
        file_size_kb,
        is_primary,
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
            row_number() over (partition by image_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where image_id is not null
    )
    where _rn = 1
)

select * from deduped
