select fu_id, account_id, feature_name, usage_date, usage_count,
    cast(date_trunc('month',usage_date) as date) as usage_month
from {{ source('saas','feature_usage') }}
