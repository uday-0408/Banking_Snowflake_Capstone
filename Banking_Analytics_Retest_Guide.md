# Banking Analytics Capstone Retest Guide

This document is designed to help you practice and memorize the necessary steps and code for your Snowflake & dbt capstone re-test. Since you must code this by hand without AI/internet assistance, the queries here are simplified to follow clear, repeatable patterns.

## 1. Project Setup & Workflow Memory

To succeed in the time limit, follow this mental checklist:
1. **Raw to Silver (dbt)**: Your `silver.banking_clean` table casts data types (dates, decimals) and cleans text fields (uppercase, trim). You already have this set up.
2. **Silver to Gold (dbt - Data Modeling)**: You need to create a Star Schema from `silver.banking_clean`:
   - `gold.dim_customer`: Unique customers (`customer_id`, `name`, `segment`, `credit_score`).
   - `gold.dim_branch`: Unique branches (`branch_id`, `branch_name`).
   - `gold.dim_date`: Unique dates (`date_key`, `year`, `month`, `quarter`).
   - `gold.fact_transaction`: Transactions (`transaction_id`, `customer_id`, `branch_id`, `date_key`, `amount`, `status`, `channel`, etc.).
   - `gold.fact_loan`: Loans (`loan_id`, `customer_id`, `branch_id`, `amount`, `interest_rate`, `tenure`, `status`).
   - `gold.fact_complaint`: Complaints (`complaint_id`, `branch_id`, `category`, `priority`, `status`).

## 2. Core SQL Patterns to Memorize

Most KPIs follow one of these three patterns. Master these, and you can solve almost any KPI:
- **Pattern 1: Aggregation with Conditional Logic (CASE WHEN)**
  *Use for Success Rates, Defaults, Priorities.*
  `SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*)`
- **Pattern 2: Time Series with Window Functions (LAG)**
  *Use for Month-over-Month Growth.*
  `(Current - Previous) / Previous * 100` where `Previous = LAG(Value) OVER(...)`
- **Pattern 3: Ranking (RANK)**
  *Use for Top Customers, Branch Rankings.*
  `RANK() OVER(PARTITION BY category ORDER BY value DESC)`

---

## 3. SQL Queries for KPIs (3 to 10)

*Note: These queries assume you have joined your `fact` tables with your `dim` tables. To make them easy to memorize, I kept table aliases consistent (`ft` for fact_transaction, `dc` for dim_customer, `db` for dim_branch).*

### KPI 3: Transaction Success Rate by Payment Channel
**Tip:** Use SUM(CASE WHEN...) for successful transactions.
```sql
SELECT 
    payment_channel,
    COUNT(transaction_id) AS total_transactions,
    SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_transactions,
    ROUND((SUM(CASE WHEN transaction_status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(transaction_id)) * 100, 2) AS success_rate
FROM gold.fact_transaction
GROUP BY payment_channel;
```

### KPI 4: Transaction Value by Customer Segment
**Tip:** Simple join and group by.
```sql
SELECT 
    dc.customer_segment,
    COUNT(ft.transaction_id) AS transaction_count,
    SUM(ft.amount) AS total_transaction_value,
    AVG(ft.amount) AS average_transaction_value
FROM gold.fact_transaction ft
JOIN gold.dim_customer dc ON ft.customer_id = dc.customer_id
GROUP BY dc.customer_segment;
```

### KPI 5: Month-over-Month Transaction Value
**Tip:** Use a CTE (WITH clause) to get monthly totals first, then apply `LAG()` in the main query.
```sql
WITH monthly_value AS (
    SELECT 
        dd.month AS transaction_month,
        dd.year AS transaction_year,
        SUM(ft.amount) AS total_transaction_value
    FROM gold.fact_transaction ft
    JOIN gold.dim_date dd ON ft.date_key = dd.date_key
    GROUP BY dd.year, dd.month
)
SELECT 
    transaction_month,
    total_transaction_value,
    LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month) AS previous_month_value,
    ROUND((total_transaction_value - LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month)) / 
          LAG(total_transaction_value) OVER (ORDER BY transaction_year, transaction_month) * 100, 2) AS mom_growth_percentage
FROM monthly_value;
```

### KPI 6: Estimated Interest Income by Loan Product
**Tip:** The formula is `amount * rate * tenure / 12`.
```sql
SELECT 
    loan_type,
    COUNT(loan_id) AS loan_count,
    SUM(loan_amount) AS total_loan_exposure,
    AVG(interest_rate) AS average_interest_rate,
    AVG(tenure_months) AS average_tenure_months,
    SUM(loan_amount * interest_rate * tenure_months / 12) AS estimated_interest
FROM gold.fact_loan
GROUP BY loan_type;
```

### KPI 7: Subprime Loan Default Rate by Branch and Loan Type
**Tip:** Filter for `credit_score < 650` and `Q2 2026`.
```sql
SELECT 
    db.branch_name,
    fl.loan_type,
    COUNT(fl.loan_id) AS total_subprime_loans,
    SUM(CASE WHEN fl.loan_status = 'DEFAULT' THEN 1 ELSE 0 END) AS defaulted_loans,
    ROUND((SUM(CASE WHEN fl.loan_status = 'DEFAULT' THEN 1 ELSE 0 END) / COUNT(fl.loan_id)) * 100, 2) AS default_rate
FROM gold.fact_loan fl
JOIN gold.dim_customer dc ON fl.customer_id = dc.customer_id
JOIN gold.dim_branch db ON fl.branch_id = db.branch_id
JOIN gold.dim_date dd ON fl.date_key = dd.date_key
WHERE dc.credit_score < 650 
  AND dd.year = 2026 
  AND dd.quarter = 2
GROUP BY db.branch_name, fl.loan_type;
```

### KPI 8: Complaint Resolution Rate by Branch and Category
```sql
SELECT 
    db.branch_name,
    fc.complaint_category,
    COUNT(fc.complaint_id) AS total_complaints,
    SUM(CASE WHEN fc.complaint_status = 'RESOLVED' THEN 1 ELSE 0 END) AS resolved_complaints,
    ROUND((SUM(CASE WHEN fc.complaint_status = 'RESOLVED' THEN 1 ELSE 0 END) / COUNT(fc.complaint_id)) * 100, 2) AS resolution_rate
FROM gold.fact_complaint fc
JOIN gold.dim_branch db ON fc.branch_id = db.branch_id
GROUP BY db.branch_name, fc.complaint_category;
```

### KPI 9: Complaint Priority Distribution Across Branches
**Tip:** Multiple `SUM(CASE WHEN...)` columns.
```sql
SELECT 
    db.branch_name,
    SUM(CASE WHEN fc.complaint_priority = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'HIGH' THEN 1 ELSE 0 END) AS high_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'LOW' THEN 1 ELSE 0 END) AS low_priority_complaints
FROM gold.fact_complaint fc
JOIN gold.dim_branch db ON fc.branch_id = db.branch_id
GROUP BY db.branch_name;
```

### KPI 10: Top 5 Customers by Net Loan Exposure per Branch
**Tip:** This is the most complex query. Use 3 CTEs: 1 for repayments, 1 for exposure calculation, 1 for ranking.
```sql
WITH loan_repayments AS (
    -- 1. Get total repayments per loan
    SELECT 
        loan_id, 
        SUM(amount) AS total_repayments
    FROM gold.fact_transaction 
    WHERE transaction_type = 'REPAYMENT'
    GROUP BY loan_id
),
customer_exposure AS (
    -- 2. Calculate net exposure
    SELECT 
        db.branch_name,
        dc.customer_id,
        dc.customer_name,
        fl.loan_type,
        fl.loan_amount,
        COALESCE(lr.total_repayments, 0) AS total_repayments,
        (fl.loan_amount - COALESCE(lr.total_repayments, 0)) AS net_loan_exposure
    FROM gold.fact_loan fl
    JOIN gold.dim_customer dc ON fl.customer_id = dc.customer_id
    JOIN gold.dim_branch db ON fl.branch_id = db.branch_id
    LEFT JOIN loan_repayments lr ON fl.loan_id = lr.loan_id
    WHERE fl.loan_status IN ('OPEN', 'RESTRUCTURED')
),
ranked_exposure AS (
    -- 3. Rank them within each branch
    SELECT 
        *,
        RANK() OVER (PARTITION BY branch_name ORDER BY net_loan_exposure DESC) AS rank_in_branch
    FROM customer_exposure
)
-- 4. Filter Top 5
SELECT * FROM ranked_exposure WHERE rank_in_branch <= 5;
```

---

## 4. Additional Monthly Performance Views

### View 3: Branch Monthly Performance View
**Tip:** Similar to the Customer Monthly View you already wrote, just partition by branch!
```sql
CREATE OR REPLACE VIEW gold.monthly_branch_performance_view AS
WITH branch_monthly AS (
    SELECT 
        db.branch_id,
        db.branch_name,
        dd.month AS transaction_month,
        dd.year AS transaction_year,
        COUNT(ft.transaction_id) AS transaction_count,
        SUM(ft.amount) AS total_transaction_amount
    FROM gold.fact_transaction ft
    JOIN gold.dim_branch db ON ft.branch_id = db.branch_id
    JOIN gold.dim_date dd ON ft.date_key = dd.date_key
    GROUP BY db.branch_id, db.branch_name, dd.year, dd.month
)
SELECT 
    branch_id,
    branch_name,
    transaction_month,
    transaction_count,
    total_transaction_amount,
    LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month) AS previous_month_amount,
    ROUND((total_transaction_amount - LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month)) / 
          LAG(total_transaction_amount) OVER (PARTITION BY branch_id ORDER BY transaction_year, transaction_month) * 100, 2) AS mom_growth_pct,
    RANK() OVER (PARTITION BY transaction_year, transaction_month ORDER BY total_transaction_amount DESC) AS branch_rank
FROM branch_monthly;
```

---

## 5. Survival Tips for the Exam
1. **Build Your Dimensions First:** Before writing KPI queries, ensure you have simple SELECT DISTINCT views/tables for your `dim_customer`, `dim_branch`, and `dim_date`. It makes querying 10x easier.
2. **Master the Date Dimension:** If you don't have a `dim_date`, you can use `DATE_TRUNC('month', transaction_date)` directly in Snowflake, but using a date dimension is better practice.
3. **Use COALESCE for NULLs:** When calculating math with joins (like KPI 10), always wrap your aggregated values in `COALESCE(col, 0)` so your math doesn't result in NULL.
4. **Window Functions Syntax:** Always remember: `FUNCTION() OVER (PARTITION BY ___ ORDER BY ___)`.
5. **Round your percentages:** Always use `ROUND((formula)*100, 2)` to get a clean percentage.

---

## 6. Advanced Topics: Idempotency, Incremental Processing, and SCDs

Your instructor mentioned checking for advanced data engineering principles if multiple datasets are loaded. Here is how to handle them:

### A. Slowly Changing Dimensions (SCD Type 2)
Instead of a standard `dim_customer.sql` model, create a snapshot in the `snapshots/` folder to track history. Memorize this syntax:

```sql
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
```
*Run it using: `dbt snapshot`*

### B. Incremental Models
For large tables like `fact_transaction`, you only want to process new data during subsequent loads. Add this config at the top, and the `is_incremental()` macro at the bottom:

```sql
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='transaction_id'
) }}

SELECT
    transaction_id,
    customer_id,
    branch_id,
    transaction_date AS date_key,
    amount,
    loaded_at
FROM {{ ref('banking_clean') }}
WHERE transaction_id IS NOT NULL

{% if is_incremental() %}
  -- Only process new data loaded since the last run
  AND loaded_at > (SELECT COALESCE(MAX(t.loaded_at), '1900-01-01') FROM {{ this }} t)
{% endif %}
```

### C. Idempotency (Safe to Rerun)
"Idempotent" means running the pipeline 1 time or 100 times produces the exact same result.
- **dbt Models**: Regular tables and views are inherently idempotent (they get dropped and recreated).
- **Incremental Models**: The `unique_key='transaction_id'` in the config above makes it idempotent. If you rerun the pipeline on the same data, dbt uses the `unique_key` to run an `UPDATE/MERGE` instead of a blind `INSERT`, preventing duplicate rows.
- **Airflow COPY INTO**: Snowflake's `COPY INTO` command is idempotent by default. It keeps track of file metadata and will skip CSV files it has already loaded unless you explicitly specify `FORCE=TRUE`.

---

## 7. The Easiest Date Dimension (`dim_date`)

Is there a better way than memorizing a giant date dimension script? **Yes!**
If you are allowed to use dbt packages, you can use the `dbt_date` package, but since you are taking a closed-book test with no internet, rely on **Snowflake's `GENERATOR` function**. It generates thousands of rows instantly and is incredibly easy to memorize.

Memorize this exact block for your `dim_date.sql`:

```sql
{{ config(materialized='table', schema='gold') }}

WITH date_spine AS (
    SELECT
        -- Starts at 2020-01-01 and generates 3650 days (10 years)
        DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS date_key
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)

SELECT
    date_key,
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(DAY FROM date_key) AS day,
    EXTRACT(QUARTER FROM date_key) AS quarter,
    DAYNAME(date_key) AS day_name,
    MONTHNAME(date_key) AS month_name
FROM date_spine
```

---

## 8. The Ultimate Capstone Master Workflow (Step-by-Step)

If you follow this exact order during your test, you will never get lost, no matter what dataset they give you.

### Step 1: Project Setup & Defaults (`dbt_project.yml`)
Don't waste time typing `{{ config(materialized='table') }}` at the top of 20 different files. Configure defaults at the folder level.
- Go to `dbt_project.yml`.
- Under `models:`, add your folder structure:
  ```yaml
  models:
    my_project:
      silver:
        +materialized: table
        +schema: silver
      gold:
        +materialized: table
        +schema: gold
        kpis:
          +materialized: view
  ```

### Step 2: Define Raw Sources (`models/source.yml`)
Tell dbt where your raw data lives in Snowflake.
- Create `source.yml`.
- Define your raw database/schema and table names.
- *Why?* This lets you use `{{ source('bronze', 'raw_data') }}` instead of hardcoding database names.

### Step 3: Build the Silver Layer (`silver/`)
Your only goal here is to clean the data and enforce formatting.
- Create `silver_clean.sql`.
- **Rules to remember here:**
  - Standardize dates using the `COALESCE + TRY_TO_DATE` trick.
  - Fix NULLs, cast `amount` to `DECIMAL(18,2)`.
  - Upper/lower case text strings.
  - Deduplicate if necessary using `QUALIFY ROW_NUMBER() OVER(...) = 1`.

### Step 4: Write Tests (`models/schema.yml`)
Before building Gold, ensure Silver is perfect.
- Create `schema.yml` in your silver folder.
- Add `not_null` and `unique` tests to primary keys (like `transaction_id`).
- Add `accepted_values` tests to status columns (e.g. `['SUCCESS', 'FAILED']`).
- Run `dbt test --select silver` to catch your bugs early!

### Step 5: Build Slowly Changing Dimensions (`snapshots/`)
If the exam requires tracking history (like credit score changes):
- Skip `dim_customer.sql`.
- Create `snapshots/dim_customer_snapshot.sql` using the SCD template from Section 6A.
- Run `dbt snapshot`.

### Step 6: Build the Gold Layer (Facts & Dims)
- **Dimensions (`dim_*.sql`)**: Simple SELECTs from Silver. `dim_branch`, `dim_account`.
- **Date Dimension (`dim_date.sql`)**: Use the `GENERATOR` trick from Section 7.
- **Facts (`fact_*.sql`)**: The transaction table. Make it `incremental` using the template from Section 6B to get extra points from your instructor!

### Step 7: Build KPIs (`gold/kpis/`)
Now that the data is perfect, write your business logic.
- Create views for the 10 KPIs.
- Rely on `SUM(CASE WHEN...)` and Window Functions.

### Step 8: Orchestrate (Airflow)
Tie it all together in a DAG.
1. `PythonOperator` -> Upload CSV to Stage
2. `PythonOperator` -> Execute `COPY INTO` from Stage to Bronze
3. `BashOperator` -> `dbt run --select silver`
4. `BashOperator` -> `dbt test --select silver`
5. `BashOperator` -> `dbt snapshot`
6. `BashOperator` -> `dbt run --select gold`

---

## 9. Surrogate Keys & Hash Diffs (Without Packages)

If the test requires generating a Surrogate Key (SK) or a Hash Diff (for detecting row changes) and you cannot use the `dbt_utils` package, you can easily build them using Snowflake's native `MD5()` and `CONCAT_WS()` functions.

### A. Surrogate Key / Hash Key
A surrogate key is a unique identifier generated by hashing the business key (or a combination of keys).

```sql
-- Generates a 32-character hexadecimal string
MD5(customer_id) AS customer_sk,

-- If your business key is a combination of two columns (e.g. branch_id + account_id)
MD5(CONCAT_WS('||', branch_id, account_id)) AS branch_account_sk
```

### B. Hash Diff
A hash diff is used in SCD Type 2 or Data Vault to quickly check if *any* attribute in a row has changed. You hash all the tracked columns together. If the hash changes between yesterday and today, the row was updated.

```sql
MD5(CONCAT_WS('||', 
    COALESCE(customer_segment, ''),
    COALESCE(CAST(credit_score AS VARCHAR), ''),
    COALESCE(kyc_status, '')
)) AS customer_hash_diff
```
*Note: Always use `COALESCE` with an empty string `''` when building a hash diff, because if even one column is `NULL`, the entire concatenation becomes `NULL` in standard SQL!*
