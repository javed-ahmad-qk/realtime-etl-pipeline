
# Databricks Structured Streaming Job: Kafka to Bronze Layer Ingestion

# This script simulates a Structured Streaming job that reads CDC events from Kafka
# and writes them as raw JSON to the Bronze layer (ADLS2).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define Kafka connection parameters
kafka_bootstrap_servers = "your_kafka_broker:9092"  # Replace with your Kafka broker(s)
kafka_topic_pattern = "mysql.banking.*"  # Debezium topics for banking tables

# Define the schema for the raw Kafka message value (assuming JSON string)
# Debezium messages typically have 'before', 'after', 'source', 'op', 'ts_ms' fields
kafka_message_schema = StructType().add("value", StringType())

# Read from Kafka using Structured Streaming
kf_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribePattern", kafka_topic_pattern) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the Kafka value (which is a JSON string from Debezium) into a string column
# The Bronze layer stores the raw JSON as a string
bronze_df = kf_df.select(col("value").cast(StringType()).alias("raw_json"))

# Define the path to the Bronze layer in ADLS2
bronze_layer_path = "abfss://bronze@youradls2account.dfs.core.windows.net/banking_cdc/"

# Write the raw JSON data to the Bronze layer as Delta tables
# Each Kafka topic (representing a MySQL table) will likely have its own path or be partitioned
query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", bronze_layer_path + "_checkpoint") \
    .toTable("bronze_banking_cdc") # Example: write to a single table, or dynamically partition

# For a more robust solution, you would dynamically write to different paths/tables
# based on the Kafka topic or Debezium 'source' field in the JSON.
# Example (conceptual, requires parsing 'raw_json' to extract topic/table name):
# query = bronze_df.writeStream \
#     .foreachBatch(lambda df, epoch_id: write_to_bronze_layer(df, epoch_id, bronze_layer_path)) \
#     .start()

# def write_to_bronze_layer(df, epoch_id, base_path):
#     # Example: extract table name from Debezium payload
#     df.withColumn("table_name", get_table_name_from_json(col("raw_json")))
#       .write.format("delta").mode("append").save(base_path + col("table_name"))

query.awaitTermination()
