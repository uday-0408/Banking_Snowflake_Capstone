{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    customer_id,
    customer_name,
    customer_segment,
    credit_score
FROM {{ ref('banking_clean') }}
WHERE customer_id IS NOT NULL
