select supplier_id, name as supplier_name, country as supplier_country,
    reliability_score, lead_time_days, category, is_preferred
from {{ source('sc','suppliers') }}
