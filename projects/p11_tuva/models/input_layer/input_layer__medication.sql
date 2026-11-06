{{ config(
     enabled = p11_tuva.tuva_boolean_var('clinical_enabled', false)
   )
}}
select *
from {{ ref('medication') }}
