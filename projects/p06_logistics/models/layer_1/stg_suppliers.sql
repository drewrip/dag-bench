select supplier_id,
    array_to_string(
        list_transform(
            string_split(lower(trim(name)), ' '),
            x -> upper(x[1:1]) || x[2:]
        ),
        ' '
    ) as supplier_name,
    country, reliability_score,
    lead_time_days, category, is_preferred
from {{ source('sc','suppliers') }}
