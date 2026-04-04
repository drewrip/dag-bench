select pcs.patient_id, sp.age_group, sp.plan_type, sp.gender,
    pcs.total_billed, pcs.total_paid, pcs.total_claims,
    cpf.chronic_diag_count, cpf.has_chronic_condition,
    ntile(10) over (order by pcs.total_paid desc) as cost_decile,
    pcs.total_paid > avg(pcs.total_paid) over () * 3 as is_high_cost
from {{ ref('patient_claim_summary') }} pcs
join {{ ref('stg_patients') }} sp using (patient_id)
left join (
    select patient_id,
        sum(chronic_diag_count) as chronic_diag_count,
        bool_or(has_chronic_condition) as has_chronic_condition
    from {{ ref('chronic_patient_flags') }}
    group by patient_id
) cpf using (patient_id)
