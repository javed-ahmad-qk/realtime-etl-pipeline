import dlt
from pyspark.sql.functions import (
    col,
    expr,
    sum,
    count,
    last,
    to_date,
    current_timestamp
)
from pyspark.sql.types import *

GOLD_LAYER_BASE_PATH = "abfss://gold@youradls2account.dfs.core.windows.net/banking_analytics/"


# ----------------------------
# CUSTOMER DIMENSION — SCD TYPE 2
# ----------------------------

@dlt.view(name="customer_dimension_changes")
def customer_dimension_changes():

    return (
        dlt.read_stream("silver_customers")
        .withWatermark("operation_timestamp_ms", "10 minutes")
        .select(
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
            col("operation_timestamp_ms").alias("effective_timestamp"),
            col("operation_type").alias("cdc_operation_type"),
            current_timestamp().alias("gold_ingestion_timestamp")
        )
    )


dlt.apply_changes(
    target="gold_customer_dimension_history",
    source="customer_dimension_changes",
    keys=["customer_id"],
    sequence_by=col("effective_timestamp"),
    apply_as_deletes=expr("cdc_operation_type = 'd'"),
    except_columns=["cdc_operation_type"],
    stored_as_scd_type="2",
    track_history_by=[
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "address_line1",
        "address_line2",
        "city",
        "state",
        "zip_code",
        "date_of_birth",
        "gender"
    ]
)


@dlt.table(
    name="gold_customer_dimension",
    comment="Current active customer dimension snapshot",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    path=f"{GOLD_LAYER_BASE_PATH}customer_dimension"
)
def gold_customer_dimension():

    return (
        dlt.read("gold_customer_dimension_history")
        .filter(col("__END_AT").isNull())
    )


# ----------------------------
# DAILY ACCOUNT BALANCES FACT
# ----------------------------

@dlt.table(
    name="gold_daily_account_balances",
    comment="Daily aggregated account balances",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    },
    path=f"{GOLD_LAYER_BASE_PATH}daily_account_balances"
)
@dlt.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
def gold_daily_account_balances():

    accounts = (
        dlt.read_stream("silver_accounts")
        .withWatermark("operation_timestamp_ms", "10 minutes")
        .withColumn("account_date", to_date(col("operation_timestamp_ms")))
    )

    return (
        accounts
        .groupBy(
            "account_date",
            "account_id",
            "customer_id",
            "account_type",
            "currency"
        )
        .agg(
            last("balance").alias("end_of_day_balance"),
            count("*").alias("daily_updates_count")
        )
    )


# ----------------------------
# DAILY TRANSACTION SUMMARY FACT
# ----------------------------

@dlt.table(
    name="gold_daily_transactions_summary",
    comment="Daily aggregated transaction summary",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    },
    path=f"{GOLD_LAYER_BASE_PATH}daily_transactions_summary"
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
def gold_daily_transactions_summary():

    transactions = (
        dlt.read_stream("silver_atm_transactions")
        .withWatermark("operation_timestamp_ms", "10 minutes")
        .withColumn(
            "transaction_date",
            to_date(col("operation_timestamp_ms"))
        )
    )

    return (
        transactions
        .groupBy(
            "transaction_date",
            "account_id",
            "transaction_type"
        )
        .agg(
            sum("amount").alias("total_transaction_amount"),
            count("transaction_id").alias("number_of_transactions")
        )
    )


# ----------------------------
# BRANCH DIMENSION
# ----------------------------

@dlt.table(
    name="gold_branch_dimension",
    comment="Branch dimension table",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true"
    },
    path=f"{GOLD_LAYER_BASE_PATH}branch_dimension"
)
@dlt.expect_or_drop("valid_branch_id", "branch_id IS NOT NULL")
def gold_branch_dimension():

    return (
        dlt.read_stream("silver_branches")
        .select(
            col("branch_id"),
            col("branch_name"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("phone_number"),
            col("operation_timestamp_ms").alias("last_updated_ms")
        )
    )
