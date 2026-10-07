{{ config(materialized='view', schema='gold') }}

WITH loan_repayments AS (
    SELECT 
        loan_id, 
        SUM(amount) AS total_repayments
    FROM {{ ref('fact_transaction') }} 
    WHERE transaction_type = 'REPAYMENT'
    GROUP BY loan_id
),
customer_exposure AS (
    SELECT 
        db.branch_name,
        dc.customer_id,
        dc.customer_name,
        fl.loan_type,
        fl.loan_amount,
        COALESCE(lr.total_repayments, 0) AS total_repayments,
        (fl.loan_amount - COALESCE(lr.total_repayments, 0)) AS net_loan_exposure
    FROM {{ ref('fact_loan') }} fl
    JOIN {{ ref('dim_customer') }} dc ON fl.customer_id = dc.customer_id
    JOIN {{ ref('dim_branch') }} db ON fl.branch_id = db.branch_id
    LEFT JOIN loan_repayments lr ON fl.loan_id = lr.loan_id
    WHERE fl.loan_status IN ('OPEN', 'RESTRUCTURED')
),
ranked_exposure AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY branch_name ORDER BY net_loan_exposure DESC) AS rank_in_branch
    FROM customer_exposure
)
SELECT * FROM ranked_exposure WHERE rank_in_branch <= 5
