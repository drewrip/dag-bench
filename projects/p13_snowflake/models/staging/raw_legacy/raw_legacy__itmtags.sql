{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'ITMTAGS') }}
),

deduped as (
    select
        tag_id,
        product_id,
        tag_name,
        tag_type,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by tag_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where tag_id is not null
    )
    where _rn = 1
)

select * from deduped
