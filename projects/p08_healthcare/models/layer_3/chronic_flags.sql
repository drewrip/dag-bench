select d.claim_id, c.patient_id,
    {{ dagbench_agg_filter('count', '*', 'd.chronic_flag') }} as chronic_count,
    count(distinct d.icd_code) as icd_count,
    {{ dagbench_fn('bool_or') }}(d.chronic_flag) as has_chronic
from {{ ref('stg_diagnoses') }} d
join {{ ref('stg_claims') }} c using (claim_id)
group by d.claim_id, c.patient_id
