select d.claim_id, c.patient_id,
    count(*) filter (where d.chronic_flag)   as chronic_diag_count,
    count(distinct d.icd_code)               as unique_icd_codes,
    bool_or(d.chronic_flag)                  as has_chronic_condition
from {{ ref('stg_diagnoses') }} d
join {{ ref('stg_claims') }} c using (claim_id)
group by d.claim_id, c.patient_id
