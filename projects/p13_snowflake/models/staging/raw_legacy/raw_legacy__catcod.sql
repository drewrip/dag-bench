{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'CATCOD') }}
),

deduped as (
    select
        category_id,
        category_code,
        category_name,
        category_description,
        parent_category_id,
        category_level,
        category_path,
        category_path_ids,
        sort_order,
        image_url,
        icon_name,
        meta_title,
        meta_description,
        meta_keywords,
        is_featured,
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
            row_number() over (partition by category_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where category_id is not null
    )
    where _rn = 1
)

select * from deduped
