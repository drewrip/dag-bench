select c.campaign_id, c.campaign_name, c.channel, c.advertiser, c.objective, c.budget,
    sum(di.impressions) as impressions, sum(di.unique_users) as reach,
    sum(di.spend) as spend,
    count(distinct ce.click_id) as clicks,
    count(distinct cve.conv_id) as conversions,
    sum(cve.revenue) as revenue,
    round(count(distinct ce.click_id)*100.0/nullif(sum(di.impressions),0),4) as ctr,
    round(count(distinct cve.conv_id)*100.0/nullif(count(distinct ce.click_id),0),4) as cvr,
    round(sum(di.spend)/nullif(count(distinct ce.click_id),0),4) as cpc,
    round(sum(di.spend)/nullif(count(distinct cve.conv_id),0),2) as cpa,
    round(sum(cve.revenue)/nullif(sum(di.spend),0),4) as roas
from {{ ref('stg_campaigns') }} c
left join {{ ref('daily_impressions') }} di using (campaign_id)
left join {{ ref('click_enriched') }} ce using (campaign_id)
left join {{ ref('conversion_enriched') }} cve using (campaign_id)
group by c.campaign_id, c.campaign_name, c.channel, c.advertiser, c.objective, c.budget
