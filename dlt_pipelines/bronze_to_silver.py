
# Databricks Delta Live Tables (DLT): Bronze to Silver Transformation

# This script defines a DLT pipeline that processes raw JSON data from the Bronze layer
# (ingested from Kafka via Debezium) and transforms it into a cleaned and conformed Silver layer.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the schema for the Debezium CDC event payload
# Example structure for a Debezium message 'value' field:
# {
#   "before": { ... },
#   "after": { ... },
#   "source": { "db": "banking", "table": "customers", ... },
#   "op": "c" (create), "u" (update), "d" (delete), "r" (read/snapshot)
#   "ts_ms": 1678886400000
# }

debezium_payload_schema = StructType([
    StructField("before", MapType(StringType(), StringType()), True), # Original state before change
    StructField("after", MapType(StringType(), StringType()), True),  # New state after change
    StructField("source", StructType([
        StructField("db", StringType(), True),
        StructField("table", StringType(), True),
        StructField("ts_ms", LongType(), True)
    ]), True),
    StructField("op", StringType(), True), # Operation type (c, u, d, r)
    StructField("ts_ms", LongType(), True) # Timestamp of the operation
])

@dlt.table(comment="Raw CDC events from Kafka, stored as JSON in Bronze.")
def bronze_raw_cdc():
    # This table represents the Bronze layer. In a real DLT pipeline,
    # this would typically be streamed from an ADLS2 path where the
    # kafka_to_bronze.py script writes its output.
    # For demonstration, we'll simulate reading from a path.
    return (spark.readStream.format("delta").table("bronze_banking_cdc"))

@dlt.table(comment="Cleaned and conformed banking data from Bronze, ready for Silver.")
def silver_banking_data():
    # Read from the bronze_raw_cdc table
    df = dlt.read_stream("bronze_raw_cdc")

    # Parse the raw_json column using the defined schema
    parsed_df = df.withColumn("parsed_payload", from_json(col("raw_json"), debezium_payload_schema))

    # Extract relevant fields and flatten the structure
    silver_df = parsed_df.select(
        col("parsed_payload.after").alias("data"),
        col("parsed_payload.before").alias("before_data"),
        col("parsed_payload.source.db").alias("source_db"),
        col("parsed_payload.source.table").alias("source_table"),
        col("parsed_payload.op").alias("operation_type"),
        col("parsed_payload.ts_ms").alias("operation_timestamp"),
        col("raw_json") # Keep raw JSON for auditing/troubleshooting
    ).filter(col("data").isNotNull()) # Filter out deletes or snapshots where 'after' is null initially

    # Further cleaning and type casting would happen here based on specific table schemas.
    # Example for a 'customers' table:
    # if col("source_table") == "customers":
    #    silver_df = silver_df.withColumn("customer_id", col("data.id").cast(LongType())) \
    #                       .withColumn("first_name", col("data.first_name").cast(StringType()))

    return silver_df


