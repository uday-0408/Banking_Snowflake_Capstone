{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    loan_id,
    customer_id,
    branch_id,
    TO_DATE(transaction_date) AS date_key,
    loan_type,
    loan_amount,
    interest_rate,
    tenure_months,
    loan_status
FROM {{ ref('banking_clean') }}
WHERE loan_id IS NOT NULL
