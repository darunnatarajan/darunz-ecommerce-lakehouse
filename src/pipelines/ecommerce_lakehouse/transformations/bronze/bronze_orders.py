"""Bronze: raw orders (JSON lines with nested items) ingested incrementally with Auto Loader.

Bronze keeps data exactly as it arrived (no cleaning) + ingestion metadata,
so every Silver fix can be replayed from here.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")


@dp.table(
    name="bronze_orders",
    comment="Raw e-commerce orders as received from the shop API (append-only).",
    table_properties={"quality": "bronze"},
)
@dp.expect("no_rescued_data", "_rescued_data IS NULL")  # warn only: surfaces schema drift in the event log
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "order_ts STRING")  # keep raw; parsed & validated in Silver
        .load(f"{LANDING}/orders")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
