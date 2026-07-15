select dept_name,
    count(*) as employees,
    count(*) filter (where attrition_risk='HIGH') as high_risk,
    count(*) filter (where attrition_risk='MEDIUM') as medium_risk,
    round(avg(risk_score),2) as avg_risk_score
from {{ ref('attrition_risk') }}
group by dept_name
