select click_id, imp_id, campaign_id, user_id, click_ts, device,
    date_trunc('day',click_ts) as click_day
from {{ source('ads','clicks') }}
