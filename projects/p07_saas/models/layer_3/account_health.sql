with accounts as (
    select account_id, name, industry, country, arr, age_days, health_score
    from {{ ref('stg_accounts') }}
),

engagement as (
    select account_id, total_events, active_days
    from {{ ref('account_engagement') }}
),

features as (
    select account_id, features_used
    from {{ ref('feature_breadth') }}
),

support as (
    select account_id, urgent_tickets, avg_csat
    from {{ ref('support_health') }}
)

select a.account_id, a.name, a.industry, a.country, a.arr, a.age_days,
    a.health_score as raw_health,
    ae.total_events, ae.active_days, fb.features_used,
    coalesce(fb.features_used,0) as feature_count,
    coalesce(sh.urgent_tickets,0) as urgent_tickets,
    coalesce(sh.avg_csat,3) as avg_csat,
    least(100,round(cast(a.health_score*0.3+least(ae.active_days,30)*0.4*(10.0/3)
        +coalesce(fb.features_used,0)*2-coalesce(sh.urgent_tickets,0)*3 as numeric(38,10)), 2)) as composite_health
from accounts a
left join engagement ae using (account_id)
left join features fb using (account_id)
left join support sh using (account_id)
