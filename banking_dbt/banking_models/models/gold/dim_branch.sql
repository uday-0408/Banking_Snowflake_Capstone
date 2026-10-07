{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    branch_id,
    branch_name
FROM {{ ref('banking_clean') }}
WHERE branch_id IS NOT NULL
