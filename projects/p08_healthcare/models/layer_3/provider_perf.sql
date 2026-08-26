select ce.provider_id, ce.provider_name, ce.specialty, ce.is_in_network,
    count(distinct ce.claim_id) as claims, sum(ce.total_billed) as billed,
    sum(ce.total_paid) as paid,
    count(*) filter (where ce.is_denied) as denied,
    round(cast(count(*) filter (where ce.is_denied)*100.0/nullif(count(*),0) as numeric(38,10)), 2) as denial_rate,
    round(cast(sum(ce.total_paid)/nullif(sum(ce.total_billed),0) as numeric(38,10)), 4) as pay_rate
from {{ ref('claims_enriched') }} ce
group by ce.provider_id, ce.provider_name, ce.specialty, ce.is_in_network
having count(*) filter (where ce.is_denied) > 0
