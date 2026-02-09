
# Databricks Delta Live Tables (DLT): Silver to Gold Transformation with SCD Type 2

# This script defines a DLT pipeline that processes cleaned data from the Silver layer
# and transforms it into aggregated, business-ready Gold layer tables. It includes
# an example of Slowly Changing Dimension (SCD) Type 2 implementation.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define a DLT table for the Silver layer data (assuming it's already defined in bronze_to_silver.py)
@dlt.table(comment="Cleaned and conformed banking data from Bronze, ready for Silver.")
def silver_banking_data():
    # This function should ideally read from the actual Silver Delta table path
    # created by the bronze_to_silver DLT pipeline.
    # For demonstration, we assume a table named 'silver_banking_data' exists.
    return spark.readStream.format("delta").table("silver_banking_data")

# Example: SCD Type 2 for Customer Dimension
# This DLT table will manage the 'gold_customers' dimension with historical changes.
@dlt.table(
    comment="Gold layer customer dimension with SCD Type 2 history.",
    table_properties={
        "quality": "gold",
        "pipeline.autoOptimize.zorderCols": "customer_id"
    }
)
def gold_customers():
    # Read from the silver_banking_data stream
    customers_df = dlt.read_stream("silver_banking_data") \
        .filter(col("source_table") == "customers") \
        .withColumn("customer_id", col("data.id").cast(LongType())) \
        .withColumn("first_name", col("data.first_name").cast(StringType())) \
        .withColumn("last_name", col("data.last_name").cast(StringType())) \
        .withColumn("email", col("data.email").cast(StringType())) \
        .withColumn("address", col("data.address").cast(StringType())) \
        .withColumn("phone", col("data.phone").cast(StringType())) \
        .withColumn("effective_date", to_timestamp(col("operation_timestamp") / 1000)) \
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "address",
            "phone",
            "effective_date",
            col("operation_type").alias("cdc_operation_type")
        )

    # Implement SCD Type 2 using dlt.apply_changes
    # This assumes 'customer_id' is the primary key and 'effective_date' is the timestamp for changes.
    dlt.apply_changes(
        target = "gold_customers_scd2", # The actual Delta table where SCD Type 2 is applied
        source = customers_df,
        keys = ["customer_id"],
        sequence_by = col("effective_date"),
        apply_as_deletes = expr("cdc_operation_type = 'd'"), # Handle deletes
        except_columns = ["cdc_operation_type"], # Exclude CDC operation type from the target table
        track_history_by = ["first_name", "last_name", "email", "address", "phone"]
    )

    # Return the current active records for the gold_customers view
    return dlt.read("gold_customers_scd2") \
        .filter(col("__END_AT").isNull())

# Example: Gold layer for aggregated transactions (Fact Table)
@dlt.table(
    comment="Gold layer aggregated banking transactions.",
    table_properties={
        "quality": "gold",
        "pipeline.autoOptimize.zorderCols": "transaction_date"
    }
)
def gold_daily_transactions():
    # Read from the silver_banking_data stream and filter for transaction tables
    transactions_df = dlt.read_stream("silver_banking_data") \
        .filter(col("source_table").isin("atm_transaction", "account_transaction")) \
        .withColumn("transaction_id", col("data.id").cast(LongType())) \
        .withColumn("account_id", col("data.account_id").cast(LongType())) \
        .withColumn("amount", col("data.amount").cast(DoubleType())) \
        .withColumn("transaction_type", col("data.type").cast(StringType())) \
        .withColumn("transaction_date", to_date(col("operation_timestamp") / 1000)) \
        .select(
            "transaction_id",
            "account_id",
            "amount",
            "transaction_type",
            "transaction_date"
        )

    # Aggregate daily transactions
    daily_summary_df = transactions_df.groupBy("transaction_date", "transaction_type") \
        .agg(
            sum("amount").alias("total_amount"),
            count("transaction_id").alias("transaction_count")
        )

    return daily_summary_df

# You would add more DLT tables here for other Gold layer entities (e.g., gold_accounts, gold_loans)
# and potentially join them to create wider fact tables or star schemas.
