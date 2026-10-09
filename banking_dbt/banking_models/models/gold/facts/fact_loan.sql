-- depends_on: {{ ref('banking_clean') }}
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='loan_id'
) }}

SELECT DISTINCT
    loan_id,
    customer_id,
    branch_id,
    TO_DATE(transaction_date) AS date_key,
    loan_type,
    loan_amount,
    interest_rate,
    tenure_months,
    loan_status,
    loaded_at
FROM {{ ref('banking_clean') }}
WHERE loan_id IS NOT NULL

{% if is_incremental() %}
  AND loaded_at > (SELECT COALESCE(MAX(t.loaded_at), '1900-01-01') FROM {{ this }} t)
{% endif %}
