select campaign_id, name as campaign_name, advertiser, channel,
    objective, start_date, end_date, budget, cpm_target,
    {{ datediff("start_date", "end_date", "day") }} as flight_days
from {{ source('ads','campaigns') }}
