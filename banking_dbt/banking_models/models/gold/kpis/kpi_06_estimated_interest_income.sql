{{ config(materialized='view', schema='gold') }}

SELECT 
    loan_type,
    COUNT(loan_id) AS loan_count,
    SUM(loan_amount) AS total_loan_exposure,
    AVG(interest_rate) AS average_interest_rate,
    AVG(tenure_months) AS average_tenure_months,
    SUM(loan_amount * interest_rate * tenure_months / 12) AS estimated_interest
FROM {{ ref('fact_loan') }}
GROUP BY loan_type
