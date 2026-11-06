{{ config(
     enabled = p11_tuva.tuva_boolean_var('claims_enabled', false)
   )
}}

{%- set tuva_columns -%}
      person_id
    , member_id
    , subscriber_id
    , subscriber_relation
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , {{ p11_tuva.quote_column('plan') }}
    , first_name
    , middle_name
    , last_name
    , name_suffix
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , phone
    , email
    , ethnicity
    , sex
    , race
    , birth_date
    , death_date
    , death_flag
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , enrollment_status
    , hospice_flag
    , institutional_snp_flag
    , medicaid_indicator
    , long_term_institutional_flag
    , part_d_raf_type
    , low_income_subsidy_indicator
    , metal_level
    , csr_indicator
    , enrollment_duration_months
    , esrd_status
    , transplant_duration_months
    , group_id
    , group_name
{%- endset -%}

{# Extension columns for testing passthrough to core.eligibility. #}
{%- set tuva_metadata -%}
    , file_date
    , file_name
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_metadata }}
from {{ source('tuva', 'synthetic_data__eligibility') }}
