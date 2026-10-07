{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    complaint_id,
    customer_id,
    branch_id,
    TO_DATE(transaction_date) AS date_key,
    complaint_category,
    complaint_priority,
    complaint_status
FROM {{ ref('banking_clean') }}
WHERE complaint_id IS NOT NULL
