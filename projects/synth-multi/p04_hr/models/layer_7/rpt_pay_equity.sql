select dept_name, female_avg, male_avg,
    round(f_m_ratio,4) as f_m_ratio,
    case when f_m_ratio<0.95 then 'Gap' when f_m_ratio>1.05 then 'Inverted' else 'Equitable' end as equity_status,
    current_timestamp as report_ts
from {{ ref('gender_pay_gaps') }}
order by f_m_ratio
