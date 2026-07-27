select imp_id, campaign_id, user_id, imp_ts,
    lower(trim(device)) as device, geo,
    lower(trim(placement)) as placement, cost_usd,
    date_trunc('hour', imp_ts) as imp_hour,
    date_trunc('day',  imp_ts) as imp_day
from {{ source('ads','impressions') }}
