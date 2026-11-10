{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'PA0001') }}
),

deduped as (
    select
        employee_id,
        employee_number,
        first_name,
        last_name,
        email,
        phone,
        hire_date,
        termination_date,
        manager_id,
        department_id,
        position_id,
        employment_type,
        status,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by employee_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where employee_id is not null
    )
    where _rn = 1
)

select * from deduped
