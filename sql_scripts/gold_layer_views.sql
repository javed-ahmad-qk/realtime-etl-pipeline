
-- SQL Views for Gold Layer Consumption (for SQL Serverless Warehouse / Databricks SQL Endpoint)
-- These views provide a simplified and optimized interface for Power BI reports.

-- View for current active customer dimension (SCD Type 2)
CREATE VIEW gold_customer_current AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address_line1,
    address_line2,
    city,
    state,
    zip_code,
    date_of_birth,
    gender,
    registration_date,
    effective_timestamp AS last_updated_at
FROM
    gold_customer_dimension
WHERE
    __END_AT IS NULL;

-- View for daily aggregated account balances
CREATE VIEW gold_daily_account_summary AS
SELECT
    account_date,
    account_id,
    customer_id,
    account_type,
    currency,
    end_of_day_balance,
    daily_updates_count
FROM
    gold_daily_account_balances;

-- View for daily aggregated transaction summary
CREATE VIEW gold_daily_transaction_summary AS
SELECT
    transaction_date,
    account_id,
    transaction_type,
    total_transaction_amount,
    number_of_transactions
FROM
    gold_daily_transactions_summary;

-- View for branch dimension
CREATE VIEW gold_branch_current AS
SELECT
    branch_id,
    branch_name,
    address,
    city,
    state,
    zip_code,
    phone_number,
    last_updated_ms
FROM
    gold_branch_dimension;

-- Add more views as needed for other Gold layer tables (e.g., loans, cards, employees)
