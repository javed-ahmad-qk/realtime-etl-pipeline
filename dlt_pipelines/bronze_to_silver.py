
# Databricks Delta Live Tables (DLT): Bronze to Silver Transformation

# This script defines a DLT pipeline that processes raw JSON data from the Bronze layer
# (ingested from Kafka via Debezium) and transforms it into a cleaned and conformed Silver layer.

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from banking_schema import BANKING_TABLE_SCHEMAS, DEBEZIUM_PAYLOAD_SCHEMA

# --- Configuration Variables (Replace with actual values or use Databricks secrets) ---
BRONZE_LAYER_BASE_PATH = "abfss://bronze@youradls2account.dfs.core.windows.net/banking_cdc/"
SILVER_LAYER_BASE_PATH = "abfss://silver@youradls2account.dfs.core.windows.net/banking_cdc/"

# Define the Bronze layer table (input for this DLT pipeline)
# This reads from the Delta tables created by the kafka_to_bronze.py job
# We assume the kafka_to_bronze job writes to separate Delta tables per source table
# in the Bronze layer, e.g., /bronze/customers, /bronze/accounts, etc.

# DLT table for Silver layer processing - one for each source table

# Function to create a DLT table for a specific banking entity
def create_silver_table(table_name, schema):
    @dlt.table(name=f"silver_{table_name}",
               comment=f"Cleaned and conformed data for {table_name} from Bronze.",
               table_properties={
                   "quality": "silver",
                   "pipeline.autoOptimize.zorderCols": "{table_name}_id" if "_id" in schema.fieldNames() else "operation_timestamp_ms"
               })
    def silver_table():
        # Read from the corresponding Bronze table
        # Note: In a real DLT pipeline, you would configure the input path for each source table
        # For this example, we assume a unified input stream that we filter.
        # A more robust DLT setup might have separate DLT pipelines or input definitions per source.
        bronze_df = dlt.read_stream("bronze_raw_cdc") \
            .filter(col("kafka_topic").contains(table_name))

        # Parse the raw_cdc_json column using the generic Debezium payload schema
        parsed_df = bronze_df.withColumn("parsed_payload", from_json(col("raw_cdc_json"), DEBEZIUM_PAYLOAD_SCHEMA))

        # Extract common CDC fields and the specific data payload
        extracted_df = parsed_df.select(
            col("parsed_payload.after").alias("after_data_map"),
            col("parsed_payload.before").alias("before_data_map"),
            col("parsed_payload.source.table").alias("source_table"),
            col("parsed_payload.op").alias("operation_type"),
            col("parsed_payload.ts_ms").alias("operation_timestamp_ms"),
            col("kafka_timestamp"),
            col("processing_timestamp")
        ).filter(col("source_table") == table_name) # Ensure we only process data for this table

        # Convert the 'after_data_map' and 'before_data_map' (which are maps) to JSON strings,
        # then parse them into structured columns using the specific table schema.
        # This handles cases where 'after' or 'before' might be null.
        silver_df = extracted_df \
            .withColumn("after_struct", from_json(to_json(col("after_data_map")), schema)) \
            .withColumn("before_struct", from_json(to_json(col("before_data_map")), schema))

        # Select all fields from 'after_struct' and 'before_struct' (prefixed for 'before')
        # and include CDC metadata
        final_silver_df = silver_df.select(
            col("after_struct.*"), # Flatten the 'after' data into columns
            *[col(f"before_struct.{f.name}").alias(f"before_{f.name}") for f in schema.fields], # Flatten 'before' data with prefix
            col("operation_type"),
            col("operation_timestamp_ms"),
            col("kafka_timestamp"),
            col("processing_timestamp")
        )
        return final_silver_df

# Dynamically create DLT tables for each banking entity
for table_name, schema in BANKING_TABLE_SCHEMAS.items():
    create_silver_table(table_name, schema)

# Define a unified Bronze raw CDC table to read from, assuming kafka_to_bronze writes to a single logical path
# or that DLT is configured to read from multiple bronze tables via a pattern.
# For simplicity, we'll define a base bronze table that all silver tables can filter from.
@dlt.table(comment="Unified Bronze raw CDC events for all banking tables.")
def bronze_raw_cdc():
    # This table should read from the base path where all raw CDC JSONs are stored.
    # In a real DLT pipeline, this would be configured to read from the ADLS2 path
    # where the kafka_to_bronze.py script writes its output for all tables.
    # Example: return spark.readStream.format("delta").load(BRONZE_LAYER_BASE_PATH)
    # For a more specific setup, you might read from a pattern like:
    return spark.readStream.format("delta").load(f"{BRONZE_LAYER_BASE_PATH}*")

