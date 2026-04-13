select imp_id, campaign_id, user_id, imp_ts, device, geo,
    placement, cost_usd,
    date_trunc('hour', imp_ts) as imp_hour,
    date_trunc('day',  imp_ts) as imp_day
from {{ source('ads','impressions') }}
