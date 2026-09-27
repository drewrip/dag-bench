select customer_id, full_name, lower(trim(email)) as email,
    upper(country) as country, signup_date, is_active,
    cast(coalesce(lifetime_spend,0.0) as numeric(12,2)) as lifetime_spend,
    {{ datediff("signup_date", "current_date", "day") }} as days_since_signup
from {{ source('raw','customers') }}
where email not ilike '%@test.%' and email not ilike '%example.com'
