select emp_id,
    count(*)                                        as review_count,
    round(avg(score),3)                             as avg_score,
    max(score)                                      as best_score,
    min(score)                                      as worst_score,
    count(*) filter (where is_high_performer)       as high_perf_count,
    count(*) filter (where is_low_performer)        as low_perf_count,
    max(review_date)                                as last_review_date
from {{ ref('stg_reviews') }}
group by emp_id
