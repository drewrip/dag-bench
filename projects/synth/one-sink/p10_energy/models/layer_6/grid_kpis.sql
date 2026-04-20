select sr.region,
    sum(sr.period_kwh)         as total_kwh,
    round(CAST(avg(sr.avg_pf) AS NUMERIC),4)    as avg_power_factor,
    sum(sr.outage_count)       as total_outages,
    sum(sr.total_cml)          as total_cml,
    avg(sr.availability_pct)   as avg_availability_pct,
    rlt.monthly_kwh            as latest_month_kwh
from {{ ref('substation_reliability') }} sr
left join (
    select region, monthly_kwh,
        row_number() over (partition by region order by month desc) as rn
    from {{ ref('region_load_trend') }}
) rlt on rlt.region = sr.region and rlt.rn = 1
group by sr.region, rlt.monthly_kwh
