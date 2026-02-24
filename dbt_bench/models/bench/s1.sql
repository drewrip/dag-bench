{{
  config(materialized='view')
}}


WITH
  final AS (
  SELECT AVG(l1.l_quantity) AS qty, AVG(l2.l_extendedprice) AS price FROM lineitem l1, lineitem l2 WHERE l1.l_orderkey = l2.l_orderkey AND l1.l_suppkey = l2.l_suppkey AND l1.l_partkey = l2.l_partkey
  )
SELECT
  *
FROM
  final


