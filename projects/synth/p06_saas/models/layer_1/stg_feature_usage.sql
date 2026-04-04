select fu_id, account_id, feature_name, usage_date, usage_count,
    date_trunc('month', usage_date) as usage_month
from {{ source('saas','feature_usage') }}
