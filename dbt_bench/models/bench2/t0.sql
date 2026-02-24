{{
  config(materialized='view')
}}

WITH
  final AS (
  SELECT l_quantity AS qty, l_extendedprice AS price FROM lineitem
  )
SELECT
  *
FROM
  final


