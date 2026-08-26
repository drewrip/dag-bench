select conv_id, click_id, campaign_id, user_id, conv_ts,
    conv_type, revenue,
    cast(date_trunc('day', conv_ts) as date) as conv_day
from {{ source('ads','conversions') }}
