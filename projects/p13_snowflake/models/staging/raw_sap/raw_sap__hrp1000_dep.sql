{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'HRP1000_DEP') }}
),

deduped as (
    select
        department_id,
        department_code,
        department_name,
        parent_department_id,
        manager_id,
        is_active,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by department_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where department_id is not null
    )
    where _rn = 1
)

select * from deduped
