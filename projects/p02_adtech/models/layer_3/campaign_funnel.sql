with campaigns as (

    select *
    from {{ ref('stg_campaigns') }}

),

daily_impressions as (

    select *
    from {{ ref('campaign_daily_impressions') }}

),

clicks as (

    select *
    from {{ ref('click_attribution') }}

),

conversions as (

    select *
    from {{ ref('conversion_with_click') }}

),

final as (

    select c.campaign_id, c.campaign_name, c.channel, c.advertiser, c.objective,
        c.budget,
        cast(sum(di.impressions) as bigint)  as total_impressions,
        cast(sum(di.unique_users) as bigint) as total_reach,
        cast(sum(di.spend) as numeric(18,6))        as total_spend,
        count(distinct ca.click_id) as total_clicks,
        count(distinct cw.conv_id) as total_conversions,
        cast(sum(cw.revenue) as numeric(18,2))            as total_revenue,
        cast(round(count(distinct ca.click_id)*100.0
              /nullif(sum(di.impressions),0), 4) as numeric(10,4)) as ctr_pct,
        cast(round(count(distinct cw.conv_id)*100.0
              /nullif(count(distinct ca.click_id),0), 4) as numeric(10,4)) as cvr_pct,
        cast(round(sum(di.spend)/nullif(count(distinct ca.click_id),0), 4) as numeric(18,4)) as cpc,
        cast(round(sum(di.spend)/nullif(count(distinct cw.conv_id),0), 2) as numeric(18,2)) as cpa,
        cast(round(sum(cw.revenue)/nullif(sum(di.spend),0), 4) as numeric(38,4)) as roas
    from campaigns c
    left join daily_impressions di on di.campaign_id = c.campaign_id
    left join clicks ca on ca.campaign_id = c.campaign_id
    left join conversions cw on cw.campaign_id = c.campaign_id
    group by c.campaign_id, c.campaign_name, c.channel, c.advertiser, c.objective, c.budget

)

select *
from final
