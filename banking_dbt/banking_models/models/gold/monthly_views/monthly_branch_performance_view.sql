-- depends_on: {{ ref('fact_transaction') }}
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('dim_branch') }}
{{ config(materialized='view', schema='gold') }}

WITH branch_monthly AS (
    SELECT 
        db.branch_id,
        db.branch_name,
        dd.month AS transaction_month,
        dd.year AS transaction_year,
        COUNT(ft.transaction_id) AS transaction_count,
        SUM(ft.amount) AS total_transaction_amount
    FROM {{ ref('fact_transaction') }} ft
    JOIN {{ ref('dim_branch') }} db ON ft.branch_id = db.branch_id
    JOIN {{ ref('dim_date') }} dd ON ft.date_key = dd.date_key
    GROUP BY db.branch_id, db.branch_name, dd.year, dd.month
)
SELECT 
    branch_id,
    branch_name,
    transaction_month,
    transaction_year,
    transaction_count,
    total_transaction_amount,
    LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month) AS previous_month_amount,
    ROUND((total_transaction_amount - LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month)) / 
          NULLIF(LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month), 0) * 100, 2) AS mom_growth_pct,
    RANK() OVER (PARTITION BY transaction_year, transaction_month ORDER BY total_transaction_amount DESC) AS branch_rank
FROM branch_monthly
