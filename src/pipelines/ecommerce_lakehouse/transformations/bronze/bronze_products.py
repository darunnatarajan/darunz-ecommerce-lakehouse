"""Bronze: product catalog change feed (CSV). Batch 1 = full catalog, later batches = price changes."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")


@dp.table(
    name="bronze_products",
    comment="Raw product change records (full rows, ordered by updated_at).",
    table_properties={"quality": "bronze"},
)
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING}/products")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
