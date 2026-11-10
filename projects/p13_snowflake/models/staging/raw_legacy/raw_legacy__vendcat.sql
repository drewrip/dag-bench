{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'VENDCAT') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        category_id,
        category_name,
        parent_category,
        status
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
