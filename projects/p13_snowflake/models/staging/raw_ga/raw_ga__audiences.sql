{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'AUDIENCES') }}
),

deduped as (
    select
        segment_id,
        segment_code,
        segment_name,
        segment_type,
        segment_description,
        segment_criteria,
        is_dynamic,
        refresh_frequency,
        last_refreshed_at,
        member_count,
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
            row_number() over (partition by segment_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where segment_id is not null
    )
    where _rn = 1
)

select * from deduped
