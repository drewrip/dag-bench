with claims as (
    select * from {{ ref('stg_claims') }}
),

patients as (
    select * from {{ ref('stg_patients') }}
),

providers as (
    select * from {{ ref('stg_providers') }}
),

final as (
    select c.*, p.gender, p.plan_type, p.state as patient_state, p.age_years, p.age_group,
        pr.specialty, pr.is_in_network, pr.provider_name
    from claims c
    join patients p using (patient_id)
    join providers pr using (provider_id)
)

select * from final
