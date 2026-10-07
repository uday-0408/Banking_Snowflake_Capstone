{{ config(materialized='view', schema='gold') }}

SELECT 
    ft.customer_id,
    dd.month as transaction_month,
    dd.year as transaction_year,
    dc.customer_name,
    dc.customer_segment,
    SUM(amount) as current_month_transaction_amount,
    LAG(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month) as previous_month_transaction_amount,
    LEAD(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month) as next_month_transaction_amount,
    ROUND(((SUM(amount) - LAG(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month)) / NULLIF(LAG(SUM(amount)) OVER(PARTITION BY ft.customer_id ORDER BY dd.year, dd.month), 0)) * 100, 2) as mom_growth_pct,
    RANK() OVER(PARTITION BY dc.customer_segment, dd.year, dd.month ORDER BY SUM(amount) DESC) as customer_rank
FROM {{ ref('fact_transaction') }} ft
JOIN {{ ref('dim_customer') }} dc ON ft.customer_id = dc.customer_id
JOIN {{ ref('dim_date') }} dd ON ft.date_key = dd.date_key
GROUP BY ft.customer_id, dd.month, dd.year, dc.customer_name, dc.customer_segment
