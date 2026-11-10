/*
================================================================================
dim_exchange_rates - Exchange Rate Dimension
================================================================================
Provides daily exchange rates for currency conversion.

Source: Finance team curated exchange rate table from Treasury system.
Rates are end-of-day rates from Bloomberg terminal.

Note: This replaced the failed attempt to use a real-time API (see fct_sales_v2_deprecated).
================================================================================
*/

{{
    config(
        materialized='table',
        tags=['dimension', 'finance', 'currency']
    )
}}

WITH date_spine AS (
    -- Generate dates for the last 5 years
    SELECT CAST(d AS DATE) AS rate_date
    FROM generate_series(CAST('2020-01-01' AS TIMESTAMP), CAST({{ dagbench_current_date() }} AS TIMESTAMP), INTERVAL '1 day') AS spine(d)
),

-- Major currency rates (simplified - production would pull from CURRENCY_EXCHANGE_RATES table)
currency_rates AS (
    SELECT
        rate_date,
        'USD' AS from_currency,
        'USD' AS to_currency,
        1.0 AS rate
    FROM date_spine

    UNION ALL

    SELECT
        rate_date,
        'EUR' AS from_currency,
        'USD' AS to_currency,
        1.08 + ({{ dagbench_random("cast(rate_date as varchar) || 'EUR'") }} * 0.05 - 0.025) AS rate  -- Simulated EUR/USD around 1.08
    FROM date_spine

    UNION ALL

    SELECT
        rate_date,
        'GBP' AS from_currency,
        'USD' AS to_currency,
        1.27 + ({{ dagbench_random("cast(rate_date as varchar) || 'GBP'") }} * 0.05 - 0.025) AS rate  -- Simulated GBP/USD around 1.27
    FROM date_spine

    UNION ALL

    SELECT
        rate_date,
        'CAD' AS from_currency,
        'USD' AS to_currency,
        0.74 + ({{ dagbench_random("cast(rate_date as varchar) || 'CAD'") }} * 0.03 - 0.015) AS rate  -- Simulated CAD/USD around 0.74
    FROM date_spine

    UNION ALL

    SELECT
        rate_date,
        'AUD' AS from_currency,
        'USD' AS to_currency,
        0.65 + ({{ dagbench_random("cast(rate_date as varchar) || 'AUD'") }} * 0.03 - 0.015) AS rate  -- Simulated AUD/USD around 0.65
    FROM date_spine

    UNION ALL

    SELECT
        rate_date,
        'JPY' AS from_currency,
        'USD' AS to_currency,
        0.0067 + ({{ dagbench_random("cast(rate_date as varchar) || 'JPY'") }} * 0.0003 - 0.00015) AS rate  -- Simulated JPY/USD
    FROM date_spine
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['rate_date', 'from_currency', 'to_currency']) }} AS exchange_rate_id,
    rate_date,
    from_currency,
    to_currency,
    {{ dagbench_round_double('rate', '6') }} AS rate,
    {{ dagbench_current_timestamp() }} AS dbt_updated_at
FROM currency_rates
