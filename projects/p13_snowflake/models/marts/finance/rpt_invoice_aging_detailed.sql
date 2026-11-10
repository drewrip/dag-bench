-- Invoice Aging Detailed
-- Detailed invoice aging

with customer_invoices as (
    select * from {{ ref('stg_finance__customer_invoices') }}
)

select
    invoice_id,
    invoice_number,
    customer_id,
    invoice_date,
    due_date,
    total_amount,
    balance_due,
    status,
    {{ dagbench_datediff('day', "due_date", (dagbench_current_date())) }} as days_overdue,
    case 
        when balance_due = 0 then 'Paid'
        when {{ dagbench_datediff('day', "due_date", (dagbench_current_date())) }} <= 0 then 'Current'
        when {{ dagbench_datediff('day', "due_date", (dagbench_current_date())) }} between 1 and 30 then '1-30 Days'
        when {{ dagbench_datediff('day', "due_date", (dagbench_current_date())) }} between 31 and 60 then '31-60 Days'
        when {{ dagbench_datediff('day', "due_date", (dagbench_current_date())) }} between 61 and 90 then '61-90 Days'
        else '90+ Days'
    end as aging_bucket
from customer_invoices
where balance_due > 0
