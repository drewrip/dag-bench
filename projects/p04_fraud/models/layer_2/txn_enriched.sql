with transactions as (

    select * from {{ ref('stg_transactions') }}

),

accounts as (

    select * from {{ ref('stg_accounts') }}

),

merchants as (

    select * from {{ ref('stg_merchants') }}

),

final as (

    select t.*, a.account_type, a.country as account_country, a.credit_limit,
        a.account_age_days, a.is_frozen,
        m.merchant_name, m.category as merchant_category, m.merchant_country,
        m.risk_tier, m.is_risky, m.avg_txn_amount,
        t.amount/nullif(m.avg_txn_amount,0) as amount_vs_avg
    from transactions t
    join accounts a using (account_id)
    join merchants m using (merchant_id)

)

select * from final
