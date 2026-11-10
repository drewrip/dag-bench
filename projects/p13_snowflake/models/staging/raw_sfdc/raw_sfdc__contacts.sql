{{
    config(
        materialized='view',
        tags=['staging', 'sfdc']
    )
}}

with source as (
    select * from {{ source('sfdc', 'CONTACTS') }}
),

deduped as (
    select
        contact_id,
        customer_id,
        contact_type,
        contact_subtype,
        contact_value,
        is_primary,
        is_verified,
        verified_at,
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
            row_number() over (partition by contact_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where contact_id is not null
    )
    where _rn = 1
)

select * from deduped
