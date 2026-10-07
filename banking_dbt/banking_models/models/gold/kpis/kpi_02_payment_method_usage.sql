{{ config(materialized='view', schema='gold') }}

SELECT
    payment_method,
    COUNT(transaction_id) as transaction_count
FROM {{ ref('fact_transaction') }}
GROUP BY payment_method
ORDER BY transaction_count DESC
