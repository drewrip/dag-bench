select pcs.patient_id, sp.age_group, sp.plan_type, sp.gender,
    pcs.total_billed, pcs.total_paid, pcs.total_claims,
    cf.chronic_count, cf.has_chronic,
    ntile(10) over (order by pcs.total_paid desc) as cost_decile,
    pcs.total_paid>avg(pcs.total_paid) over()*3 as is_high_cost
from {{ ref('patient_claim_summary') }} pcs
join {{ ref('stg_patients') }} sp using (patient_id)
left join (select patient_id, sum(chronic_count) as chronic_count,
               bool_or(has_chronic) as has_chronic
           from {{ ref('chronic_flags') }} group by patient_id) cf using (patient_id)
