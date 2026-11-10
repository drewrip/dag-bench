{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'CTRYCOD') }}
),

deduped as (
    select
        country_id,
        country_code_2,
        country_name,
        continent,
        currency_code,
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
            row_number() over (partition by country_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where country_id is not null
    )
    where _rn = 1
)

select * from deduped
