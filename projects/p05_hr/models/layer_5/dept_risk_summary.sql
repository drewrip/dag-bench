select dept_name,
    count(*) as employees,
    {{ dagbench_agg_filter('count', '*', "attrition_risk='HIGH'") }} as high_risk,
    {{ dagbench_agg_filter('count', '*', "attrition_risk='MEDIUM'") }} as medium_risk,
    cast(round(avg(risk_score), 2) as numeric(10,2)) as avg_risk_score
from {{ ref('attrition_risk') }}
group by dept_name
