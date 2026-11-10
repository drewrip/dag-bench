{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'PC2') }}
),

deduped as (
    select
        payroll_run_id,
        payroll_period,
        period_start,
        period_end,
        pay_date,
        status,
        total_gross,
        total_net,
        employee_count,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by payroll_run_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where payroll_run_id is not null
    )
    where _rn = 1
)

select * from deduped
