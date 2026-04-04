SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    {{ source('tpch', 'orders') }}
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            {{ source('tpch', 'lineitem') }}
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority
