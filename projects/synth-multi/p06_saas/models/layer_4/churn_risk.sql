select account_id, name, industry, country, arr, composite_health,
    case when composite_health<30 then 'CRITICAL' when composite_health<50 then 'AT_RISK'
         when composite_health<70 then 'NEUTRAL' else 'HEALTHY' end as risk_band,
    rank() over (order by composite_health) as risk_rank
from {{ ref('account_health') }}
