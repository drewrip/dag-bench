select c.campaign_id, c.campaign_name, c.channel, c.advertiser, c.objective,
    c.budget,
    sum(di.impressions)         as total_impressions,
    sum(di.unique_users)        as total_reach,
    sum(di.spend)               as total_spend,
    count(distinct ca.click_id) as total_clicks,
    count(distinct cw.conv_id)  as total_conversions,
    sum(cw.revenue)             as total_revenue,
    round(count(distinct ca.click_id)*100.0
          /nullif(sum(di.impressions),0),4) as ctr_pct,
    round(count(distinct cw.conv_id)*100.0
          /nullif(count(distinct ca.click_id),0),4) as cvr_pct,
    round(sum(di.spend)/nullif(count(distinct ca.click_id),0),4) as cpc,
    round(sum(di.spend)/nullif(count(distinct cw.conv_id),0),2) as cpa,
    round(sum(cw.revenue)/nullif(sum(di.spend),0),4) as roas
from {{ ref('stg_campaigns') }} c
left join {{ ref('campaign_daily_impressions') }} di using (campaign_id)
left join {{ ref('click_attribution') }} ca using (campaign_id)
left join {{ ref('conversion_with_click') }} cw using (campaign_id)
group by c.campaign_id, c.campaign_name, c.channel, c.advertiser, c.objective, c.budget
