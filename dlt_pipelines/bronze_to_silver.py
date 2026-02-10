import dlt
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    row_number,
    expr,
    current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from banking_schema import BANKING_TABLE_SCHEMAS, DEBEZIUM_PAYLOAD_SCHEMA

BRONZE_LAYER_BASE_PATH = "abfss://bronze@youradls2account.dfs.core.windows.net/banking_cdc/"
SILVER_LAYER_BASE_PATH = "abfss://silver@youradls2account.dfs.core.windows.net/banking_cdc/"


@dlt.table(
    name="bronze_raw_cdc",
    comment="Unified Bronze raw CDC events for all banking tables."
)
def bronze_raw_cdc():
    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_LAYER_BASE_PATH)
        .withColumn("ingestion_timestamp", current_timestamp())
    )


def build_silver_table(table_name: str, schema: StructType):

    primary_key = None
    for field in schema.fields:
        if field.name.endswith("_id"):
            primary_key = field.name
            break

    @dlt.table(
        name=f"silver_{table_name}",
        comment=f"Cleaned and conformed Silver table for {table_name}",
        table_properties={
            "quality": "silver",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        },
        path=f"{SILVER_LAYER_BASE_PATH}{table_name}"
    )
    @dlt.expect_or_drop("valid_operation", "operation_type IN ('c','u','d','r')")
    def silver_table():

        bronze_df = (
            dlt.read_stream("bronze_raw_cdc")
            .filter(col("kafka_topic").contains(table_name))
        )

        parsed_df = bronze_df.withColumn(
            "parsed_payload",
            from_json(col("raw_cdc_json"), DEBEZIUM_PAYLOAD_SCHEMA)
        )

        extracted_df = parsed_df.select(
            col("parsed_payload.after").alias("after_map"),
            col("parsed_payload.before").alias("before_map"),
            col("parsed_payload.source.table").alias("source_table"),
            col("parsed_payload.op").alias("operation_type"),
            col("parsed_payload.ts_ms").alias("operation_timestamp_ms"),
            col("kafka_timestamp"),
            col("processing_timestamp"),
            col("ingestion_timestamp")
        ).filter(col("source_table") == table_name)

        structured_df = (
            extracted_df
            .withColumn("after_struct", from_json(to_json(col("after_map")), schema))
            .withColumn("before_struct", from_json(to_json(col("before_map")), schema))
        )

        flattened_df = structured_df.select(
            col("after_struct.*"),
            *[
                col(f"before_struct.{f.name}").alias(f"before_{f.name}")
                for f in schema.fields
            ],
            col("operation_type"),
            col("operation_timestamp_ms"),
            col("kafka_timestamp"),
            col("processing_timestamp"),
            col("ingestion_timestamp")
        )

        if primary_key:

            window_spec = Window.partitionBy(primary_key).orderBy(
                col("operation_timestamp_ms").desc()
            )

            deduped_df = (
                flattened_df
                .withColumn("row_num", row_number().over(window_spec))
                .filter(col("row_num") == 1)
                .drop("row_num")
            )

            return deduped_df

        return flattened_df


for table_name, schema in BANKING_TABLE_SCHEMAS.items():
    build_silver_table(table_name, schema)
