-- depends_on: {{ ref('banking_clean') }}
{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    TO_DATE(transaction_date) AS date_key,
    EXTRACT(YEAR FROM transaction_date) AS year,
    EXTRACT(MONTH FROM transaction_date) AS month,
    EXTRACT(QUARTER FROM transaction_date) AS quarter
FROM {{ ref('banking_clean') }}
WHERE transaction_date IS NOT NULL
