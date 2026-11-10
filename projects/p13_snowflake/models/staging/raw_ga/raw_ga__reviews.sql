{{
    config(
        materialized='view',
        tags=['staging', 'ga']
    )
}}

with source as (
    select * from {{ source('ga', 'REVIEWS') }}
),

deduped as (
    select
        review_id,
        product_id,
        variant_id,
        customer_id,
        order_id,
        rating,
        review_title,
        review_text,
        pros,
        cons,
        is_verified_purchase,
        is_recommended,
        helpful_count,
        not_helpful_count,
        media_urls,
        status,
        moderated_at,
        moderated_by,
        rejection_reason,
        review_source,
        reviewer_display_name,
        submitted_at,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by review_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where review_id is not null
    )
    where _rn = 1
)

select * from deduped
