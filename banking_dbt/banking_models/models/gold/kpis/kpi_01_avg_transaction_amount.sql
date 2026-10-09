-- depends_on: {{ ref('fact_transaction') }}
{{ config(materialized='view', schema='gold') }}

SELECT 
    transaction_type,
    AVG(amount) as average_transaction_amount
FROM {{ ref('fact_transaction') }}
GROUP BY transaction_type
