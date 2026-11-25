"""Bronze: customer CDC feed (CSV). Each batch carries new sign-ups and profile updates."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")


@dp.table(
    name="bronze_customers",
    comment="Raw customer change records (new + updated rows, keyed by customer_id, ordered by updated_at).",
    table_properties={"quality": "bronze"},
)
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING}/customers")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
