select product_id,
    count(*) as review_count,
    cast(round(avg(rating), 2) as numeric(10,2)) as avg_rating,
    sum(helpful_votes) as total_votes,
    {{ dagbench_agg_filter('count', '*', 'is_positive') }} as pos_reviews,
    {{ dagbench_agg_filter('count', '*', 'is_negative') }} as neg_reviews,
    cast(round({{ dagbench_agg_filter('count', '*', 'is_positive') }}*100.0/nullif(count(*),0), 2) as numeric(10,2)) as pos_pct
from {{ ref('stg_reviews') }}
group by product_id
