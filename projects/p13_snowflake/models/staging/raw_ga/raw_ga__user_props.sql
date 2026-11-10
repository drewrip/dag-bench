{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'USER_PROPS') }}
),

deduped as (
    select
        preference_id,
        customer_id,
        preference_category,
        preference_key,
        preference_value,
        is_opted_in,
        effective_from,
        effective_to,
        source,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by preference_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where preference_id is not null
    )
    where _rn = 1
)

select * from deduped
