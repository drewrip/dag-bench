select a.account_id, a.name, a.industry, a.country, a.arr,
    a.account_age_days, a.health_score as raw_health_score,
    ae.total_events, ae.active_days, ae.feature_breadth,
    afb.features_used, afb.total_usage,
    ash.total_tickets, ash.avg_csat, ash.avg_ttr_hours, ash.urgent_tickets,
    -- composite health 0-100
    least(100, round(
        a.health_score * 0.3
        + least(ae.active_days, 30) * 0.4 * (10.0/3)
        + coalesce(afb.features_used,0) * 2
        - coalesce(ash.urgent_tickets,0) * 3
    ,2)) as composite_health
from {{ ref('stg_accounts') }} a
left join {{ ref('account_engagement') }} ae using (account_id)
left join {{ ref('account_feature_breadth') }} afb using (account_id)
left join {{ ref('account_support_health') }} ash using (account_id)
