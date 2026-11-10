{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'KNA1_ADDR') }}
),

deduped as (
    select
        address_id,
        customer_id,
        address_type,
        address_label,
        is_default_billing,
        is_default_shipping,
        recipient_name,
        company_name,
        address_line_1,
        address_line_2,
        address_line_3,
        city,
        state_province,
        postal_code,
        country_code,
        phone,
        delivery_instructions,
        latitude,
        longitude,
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
            row_number() over (partition by address_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where address_id is not null
    )
    where _rn = 1
)

select * from deduped
