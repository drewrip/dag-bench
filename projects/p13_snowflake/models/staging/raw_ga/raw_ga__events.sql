{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'EVENTS') }}
),

deduped as (
    select
        event_id,
        session_id,
        event_type,
        event_name,
        event_timestamp,
        page_url,
        element_id,
        element_class,
        product_id,
        event_value,
        event_data,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by event_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where event_id is not null
    )
    where _rn = 1
)

select * from deduped
