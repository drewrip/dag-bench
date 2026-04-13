select campaign_id, name as campaign_name, advertiser, channel,
    objective, start_date, end_date, budget, cpm_target,
    date_diff('day', start_date, end_date) as flight_days
from {{ source('ads','campaigns') }}
