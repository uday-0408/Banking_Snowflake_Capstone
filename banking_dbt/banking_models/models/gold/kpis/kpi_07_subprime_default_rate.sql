{{ config(materialized='view', schema='gold') }}

SELECT 
    db.branch_name,
    fl.loan_type,
    COUNT(fl.loan_id) AS total_subprime_loans,
    SUM(CASE WHEN fl.loan_status = 'DEFAULT' THEN 1 ELSE 0 END) AS defaulted_loans,
    ROUND((SUM(CASE WHEN fl.loan_status = 'DEFAULT' THEN 1 ELSE 0 END) / COUNT(fl.loan_id)) * 100, 2) AS default_rate
FROM {{ ref('fact_loan') }} fl
JOIN {{ ref('dim_customer') }} dc ON fl.customer_id = dc.customer_id
JOIN {{ ref('dim_branch') }} db ON fl.branch_id = db.branch_id
JOIN {{ ref('dim_date') }} dd ON fl.date_key = dd.date_key
WHERE dc.credit_score < 650 
  AND dd.year = 2026 
  AND dd.quarter = 2
GROUP BY db.branch_name, fl.loan_type
