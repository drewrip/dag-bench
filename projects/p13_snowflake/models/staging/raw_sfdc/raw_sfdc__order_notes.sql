{{
    config(
        materialized='view',
        tags=['staging', 'sfdc']
    )
}}

with source as (
    select * from {{ source('sfdc', 'ORDER_NOTES') }}
),

deduped as (
    select
        note_id,
        order_id,
        note_type,
        note_text,
        is_internal,
        created_by,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by note_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where note_id is not null
    )
    where _rn = 1
)

select * from deduped
