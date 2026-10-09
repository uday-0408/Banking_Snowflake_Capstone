-- depends_on: {{ ref('fact_transaction') }}
-- depends_on: {{ ref('dim_customer') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    dc.customer_segment,
    COUNT(ft.transaction_id) AS transaction_count,
    SUM(ft.amount) AS total_transaction_value,
    AVG(ft.amount) AS average_transaction_value
FROM {{ ref('fact_transaction') }} ft
JOIN {{ ref('dim_customer') }} dc ON ft.customer_id = dc.customer_id
GROUP BY dc.customer_segment
