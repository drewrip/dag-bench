select review_id, emp_id, review_date, score, category,
    score>=4.0 as is_high_performer, score<2.5 as is_low_performer,
    extract('year' from review_date) as review_year
from {{ source('hr','performance_reviews') }}
