select patient_id, dob, gender, zip_code, plan_type, state,
    date_diff('year',dob,current_date) as age_years,
    case when date_diff('year',dob,current_date)<18 then 'pediatric'
         when date_diff('year',dob,current_date)<65 then 'adult' else 'senior' end as age_group
from {{ source('hc','patients') }}
