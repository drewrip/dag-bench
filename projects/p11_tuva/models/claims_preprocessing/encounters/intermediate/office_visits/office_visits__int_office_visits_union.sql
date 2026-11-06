{{ config(
     enabled = p11_tuva.tuva_boolean_var('claims_enabled', false)
   )
}}


select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'office visit radiology' as encounter_type
, 0 as priority_number
from {{ ref('office_visits__int_office_visits_radiology') }}

{{ p11_tuva.union_distinct() }}

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'office visit surgery' as encounter_type
, 1 as priority_number
from {{ ref('office_visits__int_office_visits_surgery') }}

{{ p11_tuva.union_distinct() }}

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'office visit injections' as encounter_type
, 2 as priority_number
from {{ ref('office_visits__int_office_visits_injections') }}

{{ p11_tuva.union_distinct() }}

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'office visit pt/ot/st' as encounter_type
, 3 as priority_number
from {{ ref('office_visits__int_office_visits_ptotst') }}

{{ p11_tuva.union_distinct() }}

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'office visit' as encounter_type
, 4 as priority_number
from {{ ref('office_visits__int_office_visits_em') }}

{{ p11_tuva.union_distinct() }}

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'telehealth' as encounter_type
, 5 as priority_number
from {{ ref('office_visits__int_office_visits_telehealth') }}

{{ p11_tuva.union_distinct() }}

select claim_id
, claim_line_number
, data_source
, old_encounter_id
, 'office visit - other' as encounter_type
, 9999 as priority_number
from {{ ref('office_visits__int_office_visits') }}
