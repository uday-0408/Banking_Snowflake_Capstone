{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='transaction_id'
) }}

SELECT
    transaction_id,
    customer_id,
    branch_id,
    loan_id,
    transaction_date AS date_key,
    transaction_type,
    amount,
    payment_channel,
    payment_method,
    transaction_status,
    is_flagged_fraud,
    loaded_at
FROM {{ ref('banking_clean') }}
WHERE transaction_id IS NOT NULL

{% if is_incremental() %}
  -- Only process new data loaded since the last run
  AND loaded_at > (SELECT COALESCE(MAX(t.loaded_at), '1900-01-01') FROM {{ this }} t)
{% endif %}
