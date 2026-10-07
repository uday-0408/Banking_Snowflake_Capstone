{{ config(materialized='view', schema='gold') }}

SELECT 
    transaction_type,
    COUNT(transaction_id) as total_transactions,
    SUM(amount) as total_transaction_amount,
    AVG(amount) as average_amount,
    SUM(CASE WHEN transaction_status='SUCCESS' THEN 1 ELSE 0 END) as successful_transaction_count,
    SUM(CASE WHEN transaction_status='FAILED' THEN 1 ELSE 0 END) as failed_transaction_count,
    SUM(CASE WHEN is_flagged_fraud=True THEN 1 ELSE 0 END) as fraud_transaction_count,
    SUM(CASE WHEN is_flagged_fraud=True THEN amount ELSE 0 END) as fraud_amount,
    ROUND((SUM(CASE WHEN transaction_status='SUCCESS' THEN 1 ELSE 0 END) / COUNT(transaction_id)) * 100, 2) as success_rate
FROM {{ ref('fact_transaction') }}
GROUP BY transaction_type
