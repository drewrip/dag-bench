select conv_id, click_id, campaign_id, user_id, conv_ts,
    conv_type, revenue,
    date_trunc('day', conv_ts) as conv_day
from {{ source('ads','conversions') }}
