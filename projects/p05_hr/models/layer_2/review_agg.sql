select emp_id, count(*) as review_count, cast(round(avg(score), 3) as numeric(10,3)) as avg_score,
    cast(max(score) as numeric(4,2)) as best_score, cast(min(score) as numeric(4,2)) as worst_score,
    count(*) filter (where is_high_performer) as high_count,
    count(*) filter (where is_low_performer)  as low_count,
    max(review_date) as last_review
from {{ ref('stg_reviews') }}
group by emp_id
