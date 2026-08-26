select click_id, imp_id, campaign_id, user_id, click_ts, device,
    cast(date_trunc('day', click_ts) as date) as click_day,
    case when landing_url like '%/lp/1%' or landing_url like '%/lp/2%'
         then 'early_lp' else 'other_lp' end as landing_page_group
from {{ source('ads','clicks') }}
