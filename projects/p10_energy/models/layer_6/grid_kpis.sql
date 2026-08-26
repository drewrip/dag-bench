select sr.region, sum(sr.period_kwh) as total_kwh, round(cast(avg(sr.avg_pf) as numeric(18,6)), 4) as avg_pf,
    sum(sr.outages) as total_outages, sum(sr.total_cml) as total_cml,
    rm.monthly_kwh as latest_month_kwh
from {{ ref('substation_reliability') }} sr
left join (select region, monthly_kwh,
               row_number() over (partition by region order by month desc nulls last) as rn
           from {{ ref('region_monthly') }}) rm on rm.region=sr.region and rm.rn=1
group by sr.region, rm.monthly_kwh
