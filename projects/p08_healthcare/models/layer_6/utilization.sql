select plan_type, members, paid, cast(round(paid/nullif(members,0), 2) as numeric(18,2)) as pmpm,
    high_cost_members, cast(round(high_cost_members*100.0/nullif(members,0), 2) as numeric(10,2)) as high_cost_pct
from {{ ref('plan_cost') }}
