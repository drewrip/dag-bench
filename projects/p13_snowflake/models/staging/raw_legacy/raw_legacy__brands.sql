{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'BRANDS') }}
),

deduped as (
    select
        brand_id,
        brand_code,
        brand_name,
        brand_description,
        brand_logo_url,
        brand_website,
        parent_brand_id,
        is_private_label,
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
            row_number() over (partition by brand_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where brand_id is not null
    )
    where _rn = 1
)

select * from deduped
