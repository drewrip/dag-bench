select supplier_id,
    upper(substr(trim(name),1,1)) || lower(substr(trim(name),2)) as supplier_name,
    country, reliability_score,
    lead_time_days, category, is_preferred
from {{ source('sc','suppliers') }}
