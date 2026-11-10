{{
    config(
        materialized='view',
        tags=['staging', 'sfdc']
    )
}}

with source as (
    select * from {{ source('sfdc', 'ACCOUNT_TIERS') }}
),

deduped as (
    select
        tier_id,
        tier_code,
        tier_name,
        tier_level,
        min_points_required,
        min_spend_required,
        points_multiplier,
        discount_percentage,
        free_shipping,
        benefits_description,
        tier_color,
        tier_icon,
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
            row_number() over (partition by tier_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where tier_id is not null
    )
    where _rn = 1
)

select * from deduped
