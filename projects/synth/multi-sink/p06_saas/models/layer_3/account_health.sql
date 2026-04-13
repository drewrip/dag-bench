select a.account_id, a.name, a.industry, a.country, a.arr, a.age_days,
    a.health_score as raw_health,
    ae.total_events, ae.active_days, fb.features_used,
    coalesce(fb.features_used,0) as feature_count,
    coalesce(sh.urgent_tickets,0) as urgent_tickets,
    coalesce(sh.avg_csat,3) as avg_csat,
    least(100,round(a.health_score*0.3+least(ae.active_days,30)*0.4*(10.0/3)
        +coalesce(fb.features_used,0)*2-coalesce(sh.urgent_tickets,0)*3,2)) as composite_health
from {{ ref('stg_accounts') }} a
left join {{ ref('account_engagement') }} ae using (account_id)
left join {{ ref('feature_breadth') }} fb using (account_id)
left join {{ ref('support_health') }} sh using (account_id)
