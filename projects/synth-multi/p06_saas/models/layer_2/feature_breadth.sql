select account_id, count(distinct feature_name) as features_used,
    sum(usage_count) as total_usage, max(usage_date) as last_usage
from {{ ref('stg_feature_usage') }}
group by account_id
