select s.sub_id, s.sub_name, s.region, s.capacity_mw,
    sum(sl.total_kwh) as period_kwh, avg(sl.avg_pf) as avg_pf,
    coalesce(obr.outages,0) as outages, coalesce(obr.total_cml,0) as total_cml,
    count(distinct sl.read_day) as days_with_data
from {{ ref('stg_substations') }} s
left join {{ ref('substation_load') }} sl using (sub_id)
left join (select region, sum(outages) as outages, sum(total_cml) as total_cml
           from {{ ref('outage_by_region') }} group by region) obr on obr.region=s.region
group by s.sub_id, s.sub_name, s.region, s.capacity_mw, obr.outages, obr.total_cml
