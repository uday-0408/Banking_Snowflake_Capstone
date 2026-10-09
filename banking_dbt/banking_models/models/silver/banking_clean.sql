{{ config(
    materialized='incremental',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'banking_raw') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT COALESCE(MAX(loaded_at), '1900-01-01') FROM {{ this }})
    {% endif %}

),

cleaned AS (

    SELECT

        /* =========================================================
           TRANSACTION
           ========================================================= */

        NULLIF(TRIM(transaction_id), '') AS transaction_id,

        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'MM/DD/YYYY'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'DD-MM-YYYY'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'YYYY-MM-DD'),
            TRY_TO_TIMESTAMP_NTZ(TRIM(transaction_date), 'DD/MM/YYYY')
        ) AS transaction_date,

        UPPER(NULLIF(TRIM(transaction_type), ''))
            AS transaction_type,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(amount), ''),
            18,
            2
        ) AS amount,

        UPPER(NULLIF(TRIM(currency), ''))
            AS currency,

        UPPER(NULLIF(TRIM(transaction_status), ''))
            AS transaction_status,

        UPPER(NULLIF(TRIM(payment_channel), ''))
            AS payment_channel,

        UPPER(NULLIF(TRIM(payment_method), ''))
            AS payment_method,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(running_balance), ''),
            18,
            2
        ) AS running_balance,

        TRY_TO_BOOLEAN(
            NULLIF(TRIM(is_flagged_fraud), '')
        ) AS is_flagged_fraud,


        /* =========================================================
           MERCHANT
           ========================================================= */

        NULLIF(TRIM(merchant_name), '')
            AS merchant_name,

        UPPER(NULLIF(TRIM(merchant_category), ''))
            AS merchant_category,


        /* =========================================================
           CUSTOMER
           ========================================================= */

        NULLIF(TRIM(customer_id), '')
            AS customer_id,

        NULLIF(TRIM(customer_name), '')
            AS customer_name,

        COALESCE(
            TRY_TO_DATE(TRIM(customer_dob), 'YYYY-MM-DD'),
            TRY_TO_DATE(TRIM(customer_dob), 'MM/DD/YYYY'),
            TRY_TO_DATE(TRIM(customer_dob), 'DD-MM-YYYY'),
            TRY_TO_DATE(TRIM(customer_dob), 'DD/MM/YYYY')
        ) AS customer_dob,

        CASE
            WHEN UPPER(TRIM(customer_gender)) IN ('M', 'MALE')
                THEN 'M'

            WHEN UPPER(TRIM(customer_gender)) IN ('F', 'FEMALE')
                THEN 'F'

            WHEN UPPER(TRIM(customer_gender)) = 'OTHER'
                THEN 'OTHER'

            ELSE NULL
        END AS customer_gender,

        CASE

            WHEN LOWER(TRIM(customer_email))
                 LIKE '%_at_%'
            THEN REPLACE(
                LOWER(TRIM(customer_email)),
                '_at_',
                '@'
            )

            WHEN LOWER(TRIM(customer_email))
                 LIKE '%@%.%'
            THEN LOWER(TRIM(customer_email))

            ELSE NULL

        END AS customer_email,

        CASE

            WHEN REGEXP_LIKE(
                TRIM(customer_phone),
                '^[0-9]+\.0$'
            )
            THEN REGEXP_REPLACE(
                TRIM(customer_phone),
                '\.0$',
                ''
            )

            WHEN REGEXP_LIKE(
                TRIM(customer_phone),
                '^[0-9]+$'
            )
            THEN TRIM(customer_phone)

            ELSE NULL

        END AS customer_phone,

        NULLIF(TRIM(customer_city), '')
            AS customer_city,

        INITCAP(NULLIF(TRIM(customer_country), ''))
            AS customer_country,

        NULLIF(TRIM(customer_segment), '')
            AS customer_segment,

        NULLIF(TRIM(kyc_status), '')
            AS kyc_status,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(customer_annual_income), ''),
            18,
            2
        ) AS customer_annual_income,

        TRY_TO_NUMBER(
            NULLIF(TRIM(credit_score), '')
        ) AS credit_score,


        /* =========================================================
           ACCOUNT
           ========================================================= */

        NULLIF(TRIM(account_id), '')
            AS account_id,

        NULLIF(TRIM(account_type), '')
            AS account_type,

        COALESCE(
            TRY_TO_DATE(TRIM(account_open_date), 'YYYY-MM-DD'),
            TRY_TO_DATE(TRIM(account_open_date), 'MM/DD/YYYY'),
            TRY_TO_DATE(TRIM(account_open_date), 'DD-MM-YYYY'),
            TRY_TO_DATE(TRIM(account_open_date), 'DD/MM/YYYY')
        ) AS account_open_date,

        NULLIF(TRIM(account_status), '')
            AS account_status,


        /* =========================================================
           BRANCH
           ========================================================= */

        NULLIF(TRIM(branch_id), '')
            AS branch_id,

        NULLIF(TRIM(branch_name), '')
            AS branch_name,


        /* =========================================================
           CREDIT CARD
           ========================================================= */

        NULLIF(TRIM(credit_card_id), '')
            AS credit_card_id,

        NULLIF(TRIM(card_type), '')
            AS card_type,

        UPPER(NULLIF(TRIM(card_network), ''))
            AS card_network,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(credit_limit), ''),
            18,
            2
        ) AS credit_limit,

        NULLIF(TRIM(card_status), '')
            AS card_status,


        /* =========================================================
           LOAN
           ========================================================= */

        NULLIF(TRIM(loan_id), '')
            AS loan_id,

        NULLIF(TRIM(loan_type), '')
            AS loan_type,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(loan_amount), ''),
            18,
            2
        ) AS loan_amount,

        TRY_TO_DECIMAL(
            NULLIF(TRIM(interest_rate), ''),
            8,
            4
        ) AS interest_rate,

        TRY_TO_NUMBER(
            NULLIF(TRIM(tenure_months), '')
        ) AS tenure_months,

        NULLIF(TRIM(loan_status), '')
            AS loan_status,


        /* =========================================================
           COMPLAINT
           ========================================================= */

        NULLIF(TRIM(complaint_id), '')
            AS complaint_id,

        NULLIF(TRIM(complaint_category), '')
            AS complaint_category,

        NULLIF(TRIM(complaint_priority), '')
            AS complaint_priority,

        NULLIF(TRIM(complaint_status), '')
            AS complaint_status,


        /* =========================================================
           INGESTION METADATA
           ========================================================= */

        source_file_name,

        loaded_at

    FROM source_data

)

SELECT *
FROM cleaned