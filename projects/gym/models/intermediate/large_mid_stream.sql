SELECT
  *,
  extract(
    year
    FROM
      o_orderdate
  ) AS o_year,
  l_extendedprice * (1 - l_discount) - 42.0 * l_quantity AS amount
FROM
  {{ ref("large_mid_t2") }}
WHERE
  o_orderstatus <> 'F'
  AND l_linenumber > 1
  AND c_acctbal > 0.0
