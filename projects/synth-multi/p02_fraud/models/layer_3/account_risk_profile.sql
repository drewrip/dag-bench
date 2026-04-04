with vel as (
    select account_id, avg(txns_per_day) as avg_daily_txns,
           max(txns_per_day) as max_daily_txns, sum(flagged_count) as total_flagged
    from {{ ref('account_daily_velocity') }} group by account_id
)
select t.account_id, t.account_country, t.account_type, t.credit_limit, t.account_age_days,
    v.avg_daily_txns, v.max_daily_txns, v.total_flagged,
    count(distinct t.txn_id) as total_txns,
    sum(t.amount) as total_spend,
    count(*) filter (where t.is_flagged) as flagged_txns,
    count(*) filter (where t.is_risky) as risky_merchant_txns,
    round(v.total_flagged*100.0/nullif(count(distinct t.txn_id),0),2) as flag_rate_pct
from {{ ref('txn_enriched') }} t
join vel v using (account_id)
group by t.account_id,t.account_country,t.account_type,t.credit_limit,
         t.account_age_days,v.avg_daily_txns,v.max_daily_txns,v.total_flagged
