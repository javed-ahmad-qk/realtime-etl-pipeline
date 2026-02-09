
# Databricks Structured Streaming Job: Kafka to Bronze Layer Ingestion

# This script continuously reads CDC events from Apache Kafka, produced by Debezium,
# and writes them as raw JSON to the Bronze layer in Delta Lake format on ADLS2.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StringType, StructType, StructField, TimestampType
import os

# Initialize Spark Session with Delta Lake and ADLS2 configurations
spark = SparkSession.builder \
    .appName("FinStream_KafkaToBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

# --- Configuration Variables (Replace with actual values or use Databricks secrets) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "your_kafka_broker_1:9092,your_kafka_broker_2:9092")
KAFKA_TOPIC_PATTERN = os.getenv("KAFKA_TOPIC_PATTERN", "dbserver1.banking_db.*") # Debezium topic prefix
BRONZE_LAYER_BASE_PATH = os.getenv("BRONZE_LAYER_BASE_PATH", "abfss://bronze@youradls2account.dfs.core.windows.net/banking_cdc/")
CHECKPOINT_BASE_PATH = os.getenv("CHECKPOINT_BASE_PATH", "abfss://checkpoints@youradls2account.dfs.core.windows.net/finstream_cdc_bronze/")

# Schema for the raw Kafka message (key and value are typically strings from Debezium)
# We only care about the value for the Bronze layer
kafka_read_schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", TimestampType(), True)
])

print(f"Starting Kafka to Bronze ingestion from {KAFKA_TOPIC_PATTERN} on {KAFKA_BOOTSTRAP_SERVERS}")

# Read from Kafka using Structured Streaming
raw_kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribePattern", KAFKA_TOPIC_PATTERN) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Select the raw JSON value and add metadata for partitioning/auditing
bronze_df = raw_kafka_stream_df.select(
    col("value").cast(StringType()).alias("raw_cdc_json"),
    col("topic").alias("kafka_topic"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("processing_timestamp")
)

# Function to dynamically write to Bronze Delta tables based on Kafka topic
# Debezium topics are typically in the format: <db.server.name>.<database_name>.<table_name>
# We extract the table name for partitioning/table naming.

def write_to_bronze_layer(df, epoch_id):
    # Extract table name from Kafka topic
    # Example: dbserver1.banking_db.customers -> customers
    df_with_table_name = df.withColumn("table_name", split(col("kafka_topic"), "\\.").getItem(2))

    # Write to ADLS2 Bronze layer, partitioned by table_name and date
    # This ensures each source table has its own Delta table in the Bronze layer
    table_names = [row.table_name for row in df_with_table_name.select("table_name").distinct().collect()]

    for table_name in table_names:
        if table_name:
            print(f"Writing epoch {epoch_id} for table: {table_name}")
            (df_with_table_name.filter(col("table_name") == table_name)
             .write.format("delta")
             .mode("append")
             .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}{table_name}/")
             .save(f"{BRONZE_LAYER_BASE_PATH}{table_name}/"))

# Start the streaming query
query = bronze_df.writeStream \
    .foreachBatch(write_to_bronze_layer) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
