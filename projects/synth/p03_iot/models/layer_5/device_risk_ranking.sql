select dh.*,
    rank() over (partition by region order by health_score asc)  as risk_rank_in_region,
    ntile(5) over (order by health_score asc)                    as risk_quintile,
    case
        when health_score < 40  then 'CRITICAL'
        when health_score < 60  then 'WARNING'
        when health_score < 80  then 'FAIR'
        else 'GOOD'
    end as health_band
from {{ ref('device_health_score') }} dh
