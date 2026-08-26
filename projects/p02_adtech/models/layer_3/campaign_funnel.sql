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
        sum(di.impressions)  as total_impressions,
        sum(di.unique_users) as total_reach,
        sum(di.spend)        as total_spend,
        count(distinct ca.click_id) as total_clicks,
        count(distinct cw.conv_id) as total_conversions,
        sum(cw.revenue)            as total_revenue,
        round(cast(count(distinct ca.click_id)*100.0
              /nullif(sum(di.impressions),0) as numeric(38,10)), 4) as ctr_pct,
        round(cast(count(distinct cw.conv_id)*100.0
              /nullif(count(distinct ca.click_id),0) as numeric(38,10)), 4) as cvr_pct,
        round(cast(sum(di.spend)/nullif(count(distinct ca.click_id),0) as numeric(38,10)), 4) as cpc,
        round(cast(sum(di.spend)/nullif(count(distinct cw.conv_id),0) as numeric(38,10)), 2) as cpa,
        round(cast(sum(cw.revenue)/nullif(sum(di.spend),0) as numeric(38,10)), 4) as roas
    from campaigns c
    left join daily_impressions di using (campaign_id)
    left join clicks ca using (campaign_id)
    left join conversions cw using (campaign_id)
    group by c.campaign_id, c.campaign_name, c.channel, c.advertiser, c.objective, c.budget

)

select *
from final
