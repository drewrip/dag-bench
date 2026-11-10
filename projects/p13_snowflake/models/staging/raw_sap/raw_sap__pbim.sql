{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'PBIM') }}
),

deduped as (
    select
        forecast_id,
        variant_id,
        warehouse_id,
        forecast_date,
        forecast_period,
        forecasted_demand,
        lower_bound,
        upper_bound,
        confidence_level,
        forecast_model,
        generated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by forecast_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where forecast_id is not null
    )
    where _rn = 1
)

select * from deduped
