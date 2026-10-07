{{ config(materialized='view', schema='gold') }}

WITH monthly_value AS (
    SELECT 
        dd.month AS transaction_month,
        dd.year AS transaction_year,
        SUM(ft.amount) AS total_transaction_value
    FROM {{ ref('fact_transaction') }} ft
    JOIN {{ ref('dim_date') }} dd ON ft.date_key = dd.date_key
    GROUP BY dd.year, dd.month
)
SELECT 
    transaction_month,
    total_transaction_value,
    LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month) AS previous_month_value,
    ROUND((total_transaction_value - LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month)) / 
          LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month) * 100, 2) AS mom_growth_percentage
FROM monthly_value
