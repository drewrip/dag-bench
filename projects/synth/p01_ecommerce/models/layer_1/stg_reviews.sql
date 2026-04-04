select review_id, product_id, customer_id, rating, review_date,
    coalesce(helpful_votes,0) as helpful_votes,
    rating >= 4 as is_positive, rating <= 2 as is_negative
from {{ source('raw','reviews') }}
