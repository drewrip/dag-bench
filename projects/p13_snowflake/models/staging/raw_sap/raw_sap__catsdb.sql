{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'CATSDB') }}
),

deduped as (
    select
        entry_id,
        employee_id,
        entry_date,
        clock_in,
        clock_out,
        break_minutes,
        hours_worked,
        entry_type,
        status,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by entry_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where entry_id is not null
    )
    where _rn = 1
)

select * from deduped
