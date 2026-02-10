from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    split,
    to_date
)
from pyspark.sql.types import StringType
import os

spark = (
    SparkSession.builder
    .appName("FinStream_KafkaToBronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .config("spark.sql.streaming.stateStore.providerClass",
            "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "your_kafka_broker_1:9092,your_kafka_broker_2:9092"
)

KAFKA_TOPIC_PATTERN = os.getenv(
    "KAFKA_TOPIC_PATTERN",
    "dbserver1.banking_db.*"
)

BRONZE_LAYER_BASE_PATH = os.getenv(
    "BRONZE_LAYER_BASE_PATH",
    "abfss://bronze@youradls2account.dfs.core.windows.net/banking_cdc/"
)

CHECKPOINT_BASE_PATH = os.getenv(
    "CHECKPOINT_BASE_PATH",
    "abfss://checkpoints@youradls2account.dfs.core.windows.net/finstream_cdc_bronze/"
)


print(f"Kafka ingestion started: {KAFKA_TOPIC_PATTERN}")


raw_kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribePattern", KAFKA_TOPIC_PATTERN)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
)


bronze_df = (
    raw_kafka_stream_df
    .select(
        col("value").cast(StringType()).alias("raw_cdc_json"),
        col("topic").alias("kafka_topic"),
        col("timestamp").alias("kafka_timestamp")
    )
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn(
        "table_name",
        split(col("kafka_topic"), "\\.").getItem(2)
    )
    .withColumn(
        "ingestion_date",
        to_date(col("processing_timestamp"))
    )
)


def write_to_bronze_layer(batch_df, epoch_id):

    if batch_df.isEmpty():
        return

    (
        batch_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("table_name", "ingestion_date")
        .option("mergeSchema", "true")
        .save(BRONZE_LAYER_BASE_PATH)
    )


query = (
    bronze_df.writeStream
    .foreachBatch(write_to_bronze_layer)
    .option("checkpointLocation", CHECKPOINT_BASE_PATH)
    .trigger(processingTime="30 seconds")
    .outputMode("append")
    .start()
)

query.awaitTermination()
