{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'ITMATTR') }}
),

deduped as (
    select
        attribute_id,
        attribute_code,
        attribute_name,
        attribute_description,
        attribute_type,
        data_type,
        is_variant_attribute,
        is_filterable,
        is_searchable,
        is_comparable,
        is_required,
        default_value,
        validation_regex,
        min_value,
        max_value,
        display_order,
        attribute_group,
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
            row_number() over (partition by attribute_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where attribute_id is not null
    )
    where _rn = 1
)

select * from deduped
