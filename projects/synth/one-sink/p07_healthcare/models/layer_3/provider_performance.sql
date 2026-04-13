select ce.provider_id, ce.provider_name, ce.specialty, ce.is_in_network,
    count(distinct ce.claim_id)                  as total_claims,
    sum(ce.total_billed)                         as total_billed,
    sum(ce.total_paid)                           as total_paid,
    count(*) filter (where ce.is_denied)         as denied_count,
    round(count(*) filter (where ce.is_denied)
          *100.0/nullif(count(*),0),2)           as denial_rate_pct,
    round(avg(ce.total_billed),2)                as avg_claim_size,
    round(sum(ce.total_paid)/nullif(sum(ce.total_billed),0),4) as pay_rate
from {{ ref('claims_enriched') }} ce
group by ce.provider_id, ce.provider_name, ce.specialty, ce.is_in_network
