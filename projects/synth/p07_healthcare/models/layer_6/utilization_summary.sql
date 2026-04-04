select pca.plan_type,
    pca.members, pca.plan_paid, pca.high_cost_members,
    round(pca.plan_paid / nullif(pca.members,0),2) as pmpm,
    round(pca.high_cost_members*100.0/nullif(pca.members,0),2) as high_cost_pct
from {{ ref('plan_cost_analysis') }} pca
