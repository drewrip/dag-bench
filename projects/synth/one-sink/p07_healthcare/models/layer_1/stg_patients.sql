select patient_id, dob, gender, zip_code, plan_type, state,
    {{ datediff("dob", "current_date", "year") }} as age_years,
    case
        when {{ datediff("dob", "current_date", "year") }} < 18 then 'pediatric'
        when {{ datediff("dob", "current_date", "year") }} < 65 then 'adult'
        else 'senior'
    end as age_group
from {{ source('hc','patients') }}
