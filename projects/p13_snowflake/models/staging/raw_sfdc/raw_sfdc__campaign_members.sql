{{
    config(
        materialized='view',
        tags=['staging', 'sfdc']
    )
}}

with source as (
    select * from {{ source('sfdc', 'CAMPAIGN_MEMBERS') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        member_id,
        campaign_id,
        contact_id,
        status,
        first_responded_date,
        created_date
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
