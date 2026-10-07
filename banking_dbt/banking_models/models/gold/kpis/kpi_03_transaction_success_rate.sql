{{ config(materialized='view', schema='gold') }}

SELECT 
    payment_channel,
    COUNT(transaction_id) AS total_transactions,
    SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    ROUND((SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(transaction_id)) * 100, 2) AS success_rate
FROM {{ ref('fact_transaction') }}
GROUP BY payment_channel
