select product_id,
    count(*) as review_count,
    round(avg(rating),2) as avg_rating,
    sum(helpful_votes) as total_votes,
    count(*) filter (where is_positive) as pos_reviews,
    count(*) filter (where is_negative) as neg_reviews,
    round(count(*) filter (where is_positive)*100.0/nullif(count(*),0),2) as pos_pct
from {{ ref('stg_reviews') }}
group by product_id
