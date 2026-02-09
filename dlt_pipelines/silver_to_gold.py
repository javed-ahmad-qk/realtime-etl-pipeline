
# Databricks Delta Live Tables (DLT): Silver to Gold Transformation with SCD Type 2

# This script defines a DLT pipeline that processes cleaned data from the Silver layer
# and transforms it into aggregated, business-ready Gold layer tables. It includes
# an example of Slowly Changing Dimension (SCD) Type 2 implementation for the customer dimension.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from banking_schema import BANKING_TABLE_SCHEMAS

# --- Configuration Variables (Replace with actual values or use Databricks secrets) ---
GOLD_LAYER_BASE_PATH = "abfss://gold@youradls2account.dfs.core.windows.net/banking_analytics/"

# Define DLT tables for Gold layer entities

# 1. Gold Customer Dimension (SCD Type 2)
@dlt.table(
    name="gold_customer_dimension",
    comment="Gold layer customer dimension with SCD Type 2 history tracking.",
    table_properties={
        "quality": "gold",
        "pipeline.autoOptimize.zorderCols": "customer_id"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def gold_customer_dimension():
    # Read from the silver_customers table (created by bronze_to_silver DLT)
    silver_customers_df = dlt.read_stream("silver_customers")

    # Select and cast relevant columns for the customer dimension
    customer_data = silver_customers_df.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone_number"),
        col("address_line1"),
        col("address_line2"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("date_of_birth"),
        col("gender"),
        col("registration_date"),
        col("last_update_timestamp").alias("effective_timestamp"), # Use this for SCD2 sequence
        col("operation_type").alias("cdc_operation_type")
    )

    # Apply SCD Type 2 using dlt.apply_changes
    # The target table will be named gold_customer_dimension_scd2 internally by DLT
    dlt.apply_changes(
        target = "gold_customer_dimension_scd2",
        source = customer_data,
        keys = ["customer_id"],
        sequence_by = col("effective_timestamp"),
        apply_as_deletes = expr("cdc_operation_type = 'd'"),
        except_columns = ["cdc_operation_type"],
        track_history_by = [
            "first_name", "last_name", "email", "phone_number",
            "address_line1", "address_line2", "city", "state", "zip_code",
            "date_of_birth", "gender"
        ]
    )

    # Return the current active records for the gold_customer_dimension view
    return dlt.read("gold_customer_dimension_scd2") \
        .filter(col("__END_AT").isNull())

# 2. Gold Account Fact Table (Aggregated Daily Balances)
@dlt.table(
    name="gold_daily_account_balances",
    comment="Gold layer daily aggregated account balances.",
    table_properties={
        "quality": "gold",
        "pipeline.autoOptimize.zorderCols": "account_date"
    }
)
@dlt.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
def gold_daily_account_balances():
    # Read from the silver_accounts table
    silver_accounts_df = dlt.read_stream("silver_accounts")

    # Aggregate daily balances
    daily_balances = silver_accounts_df \
        .withColumn("account_date", to_date(col("last_activity_date"))) \
        .groupBy("account_date", "account_id", "customer_id", "account_type", "currency") \
        .agg(
            last("balance").alias("end_of_day_balance"), # Get last balance for the day
            count("account_id").alias("daily_updates_count")
        )

    return daily_balances

# 3. Gold Transaction Fact Table (Aggregated Daily Transactions)
@dlt.table(
    name="gold_daily_transactions_summary",
    comment="Gold layer daily aggregated transaction summary.",
    table_properties={
        "quality": "gold",
        "pipeline.autoOptimize.zorderCols": "transaction_date"
    }
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
def gold_daily_transactions_summary():
    # Read from silver_atm_transactions and other potential transaction tables
    silver_atm_transactions_df = dlt.read_stream("silver_atm_transactions")

    # Union with other transaction types if available (e.g., silver_online_transactions)
    # For simplicity, we'll just use ATM transactions here.
    all_transactions_df = silver_atm_transactions_df

    daily_transactions = all_transactions_df \
        .withColumn("transaction_date", to_date(col("transaction_timestamp"))) \
        .groupBy("transaction_date", "account_id", "transaction_type") \
        .agg(
            sum("amount").alias("total_transaction_amount"),
            count("transaction_id").alias("number_of_transactions")
        )

    return daily_transactions

# You would continue to define more Gold layer tables here for other entities
# (e.g., loans, cards, branches) based on your analytical requirements.
# For example, a gold_loan_fact table, gold_card_fact table, etc.

# Example: Gold Branch Dimension
@dlt.table(
    name="gold_branch_dimension",
    comment="Gold layer branch dimension.",
    table_properties={
        "quality": "gold"
    }
)
@dlt.expect_or_drop("valid_branch_id", "branch_id IS NOT NULL")
def gold_branch_dimension():
    # Read from the silver_branches table
    silver_branches_df = dlt.read_stream("silver_branches")

    # Select and cast relevant columns for the branch dimension
    branch_data = silver_branches_df.select(
        col("branch_id"),
        col("branch_name"),
        col("address"),
        col("city"),
        col("state"),
        col("zip_code"),
        col("phone_number"),
        col("operation_timestamp_ms").alias("last_updated_ms")
    )
    return branch_data

