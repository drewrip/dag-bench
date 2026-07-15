select po.*, s.supplier_name, s.reliability_score,
    round((po.received_qty-po.ordered_qty)*100.0/nullif(po.ordered_qty,0),2) as fill_rate_pct
from {{ ref('stg_pos') }} po
join {{ ref('stg_suppliers') }} s using (supplier_id)
