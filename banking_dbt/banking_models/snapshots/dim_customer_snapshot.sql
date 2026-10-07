{% snapshot snapshot_dim_customer %}

{{
    config(
      target_schema='gold',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_segment', 'credit_score', 'kyc_status', 'customer_annual_income', 'customer_city']
    )
}}

SELECT
    customer_id,
    customer_name,
    customer_segment,
    credit_score,
    kyc_status,
    customer_annual_income,
    customer_city,
    customer_country
FROM {{ ref('banking_clean') }}
WHERE customer_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY transaction_date DESC) = 1

{% endsnapshot %}
