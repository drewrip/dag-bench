select dept_name,
    count(*)                                           as total_employees,
    count(*) filter (where attrition_risk='HIGH')      as high_risk_count,
    count(*) filter (where attrition_risk='MEDIUM')    as medium_risk_count,
    round(avg(risk_score),2)                           as avg_risk_score,
    round(avg(total_comp),2)                           as avg_total_comp
from {{ ref('attrition_risk') }}
group by dept_name
