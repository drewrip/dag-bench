with ProspectRaw as (
  select
    *
  from
    {{ source('tpcdi', 'raw_prospect') }}
),
p AS (
    SELECT
        *,
        CASE WHEN (
                (CASE WHEN networth::FLOAT > 1000000 OR income::FLOAT > 200000 THEN 'HighValue+' ELSE '' END) ||
                (CASE WHEN numberchildren::INT > 3 OR numbercreditcards::INT > 5 THEN 'Expenses+' ELSE '' END) ||
                (CASE WHEN age::INT > 45 THEN 'Boomer+' ELSE '' END) ||
                (CASE WHEN income::FLOAT < 50000 OR creditrating::INT < 600 OR networth::FLOAT < 100000 THEN 'MoneyAlert+' ELSE '' END) ||
                (CASE WHEN numbercars::INT > 3 OR numbercreditcards::INT > 7 THEN 'Spender+' ELSE '' END) ||
                (CASE WHEN age::INT < 25 AND networth::FLOAT > 1000000 THEN 'Inherited+' ELSE '' END)
            ) <> '' THEN
            LEFT(
                (CASE WHEN networth::FLOAT > 1000000 OR income::FLOAT > 200000 THEN 'HighValue+' ELSE '' END) ||
                (CASE WHEN numberchildren::INT > 3 OR numbercreditcards::INT > 5 THEN 'Expenses+' ELSE '' END) ||
                (CASE WHEN age::INT > 45 THEN 'Boomer+' ELSE '' END) ||
                (CASE WHEN income::FLOAT < 50000 OR creditrating::INT < 600 OR networth::FLOAT < 100000 THEN 'MoneyAlert+' ELSE '' END) ||
                (CASE WHEN numbercars::INT > 3 OR numbercreditcards::INT > 7 THEN 'Spender+' ELSE '' END) ||
                (CASE WHEN age::INT < 25 AND networth::FLOAT > 1000000 THEN 'Inherited+' ELSE '' END),
                GREATEST(
                    LENGTH(
                        CONCAT_WS('',
                            CASE WHEN networth::FLOAT > 1000000 OR income::FLOAT > 200000 THEN 'HighValue+' ELSE NULL END,
                            CASE WHEN numberchildren::INT > 3 OR numbercreditcards::INT > 5 THEN 'Expenses+' ELSE NULL END,
                            CASE WHEN age::INT > 45 THEN 'Boomer+' ELSE NULL END,
                            CASE WHEN income::FLOAT < 50000 OR creditrating::INT < 600 OR networth::FLOAT < 100000 THEN 'MoneyAlert+' ELSE NULL END,
                            CASE WHEN numbercars::INT > 3 OR numbercreditcards::INT > 7 THEN 'Spender+' ELSE NULL END,
                            CASE WHEN age::INT < 25 AND networth::FLOAT > 1000000 THEN 'Inherited+' ELSE NULL END
                        )
                    ) - 1,
                    0
                )
            )
            ELSE NULL
        END AS marketingnameplate
    FROM ProspectRaw
)
SELECT * FROM (
  SELECT
    * EXCLUDE(batchid),
    max(batchid) recordbatchid,
    min(batchid) batchid
  FROM p
  GROUP BY ALL
)
WHERE recordbatchid = 3
