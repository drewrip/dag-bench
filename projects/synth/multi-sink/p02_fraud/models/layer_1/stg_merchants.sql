select merchant_id, name as merchant_name, category, country as merchant_country,
    risk_tier, avg_txn_amount,
    risk_tier in ('high','critical') as is_risky
from {{ source('src','merchants') }}
