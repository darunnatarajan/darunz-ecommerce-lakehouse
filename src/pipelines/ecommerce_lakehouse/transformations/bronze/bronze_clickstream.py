"""Bronze: raw web/app clickstream events (JSON lines) via Auto Loader."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")


@dp.table(
    name="bronze_clickstream",
    comment="Raw clickstream events (page_view -> purchase funnel), append-only.",
    table_properties={"quality": "bronze"},
)
@dp.expect("no_rescued_data", "_rescued_data IS NULL")
def bronze_clickstream():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "event_ts STRING")
        .load(f"{LANDING}/clickstream")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
