select customer_id, full_name, lower(trim(email)) as email,
    upper(country) as country, signup_date, is_active,
    coalesce(lifetime_spend, 0.0) as lifetime_spend,
    {{ datediff("signup_date", "current_date", "day") }} as days_since_signup
from {{ source('raw','customers') }}
