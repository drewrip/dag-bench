{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'SESSIONS') }}
),

deduped as (
    select
        session_id,
        visitor_id,
        customer_id,
        channel_id,
        session_start,
        session_end,
        duration_seconds,
        page_views,
        landing_page,
        exit_page,
        referrer,
        utm_source,
        utm_medium,
        utm_campaign,
        device_type,
        browser,
        os,
        ip_address,
        country,
        is_converted,
        order_id,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by session_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where session_id is not null
    )
    where _rn = 1
)

select * from deduped
