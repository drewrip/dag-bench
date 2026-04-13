-- layer_2/product_review_summary.sql
select
    r.product_id,
    count(*)                                           as review_count,
    round(avg(r.rating), 2)                            as avg_rating,
    sum(r.helpful_votes)                               as total_helpful_votes,
    count(*) filter (where r.is_positive)              as positive_reviews,
    count(*) filter (where r.is_negative)              as negative_reviews,
    round(
        count(*) filter (where r.is_positive) * 100.0
        / nullif(count(*), 0), 2
    )                                                  as positive_pct,
    max(r.review_date)                                 as last_review_date,
    min(r.review_date)                                 as first_review_date
from {{ ref('stg_reviews') }} r
group by r.product_id
