select ah.account_id, ah.name, ah.industry, ah.country, ah.arr, ah.composite_health,
    case when ah.composite_health<30 then 'CRITICAL' when ah.composite_health<50 then 'AT_RISK'
         when ah.composite_health<70 then 'NEUTRAL' else 'HEALTHY' end as risk_band,
    rank() over (order by ah.composite_health) as risk_rank
from {{ ref('account_health') }} ah
where exists (
    select 1 from {{ ref('stg_subscriptions') }} s
    where s.account_id = ah.account_id and s.is_active
)
