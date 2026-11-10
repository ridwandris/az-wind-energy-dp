from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "kelmarsh_wind"
SCHEMA = "bronze"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/kelmarsh_landing_vol"
CHECKPOINT_ROOT = f"{VOLUME_ROOT}/_checkpoints"


def strip_kelmarsh_preamble(directory_path: str) -> None:
    """
    Kelmarsh CSVs contain 9 lines of '#' comments followed by a header row
    starting with '# Date and time' or '# Timestamp'. This function strips
    the comment lines once and normalizes the header row so Spark Auto Loader
    parses the column names cleanly.
    """
    base = Path(directory_path)
    if not base.exists():
        return

    for csv_file in base.rglob("*.csv"):
        with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline()

        if not first_line.startswith("#"):
            continue

        print(f"Normalizing header preamble in: {csv_file.name}")
        with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        cleaned_lines = []
        header_found = False
        for line in lines:
            if not header_found:
                if line.startswith(("# Date and time", "# Timestamp")):
                    cleaned_lines.append(line.lstrip("# ").lstrip("#"))
                    header_found = True
                elif not line.startswith("#"):
                    cleaned_lines.append(line)
                    header_found = True
            else:
                cleaned_lines.append(line)

        with open(csv_file, "w", encoding="utf-8") as f:
            f.writelines(cleaned_lines)


def ingest_stream_with_autoloader(
    spark: SparkSession,
    source_subdir: str,
    target_table: str,
) -> None:
    """Ingest CSV directory incrementally using Databricks Auto Loader (cloudFiles)."""
    source_path = f"{VOLUME_ROOT}/{source_subdir}"
    schema_location = f"{CHECKPOINT_ROOT}/{target_table}_schema"
    checkpoint_location = f"{CHECKPOINT_ROOT}/{target_table}_checkpoint"
    full_table_name = f"{CATALOG}.{SCHEMA}.{target_table}"

    print(f"Checking CSV headers in {source_path}...")
    strip_kelmarsh_preamble(source_path)

    print(f"Starting Auto Loader stream into {full_table_name}...")
    df_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(source_path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn(
            "turbine_id",
            F.regexp_extract(F.col("_metadata.file_path"), r"Kelmarsh_(\d+)_", 1).cast(
                "int"
            ),
        )
        .withColumn(
            "file_year",
            F.regexp_extract(F.col("_metadata.file_path"), r"year=(\d{4})", 1).cast(
                "int"
            ),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    query = (
        df_stream.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .trigger(availableNow=True)
        .toTable(full_table_name)
    )

    query.awaitTermination()
    row_count = spark.table(full_table_name).count()
    print(
        f"Completed Auto Loader ingestion for {full_table_name}. Total rows: {row_count}"
    )


def ingest_static_metadata(spark: SparkSession) -> None:
    """Ingest static turbine metadata CSV into a Bronze Delta table."""
    static_path = f"{VOLUME_ROOT}/static/Kelmarsh_WT_static.csv"
    full_table_name = f"{CATALOG}.{SCHEMA}.turbine_static_raw"

    print(f"Ingesting static turbine metadata into {full_table_name}...")
    df_static = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(static_path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
    )

    (
        df_static.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(full_table_name)
    )
    print(f"Completed static table ingestion: {full_table_name}")


def main() -> None:
    spark = SparkSession.builder.appName("Kelmarsh_Bronze_Ingestion").getOrCreate()

    ingest_static_metadata(spark)
    ingest_stream_with_autoloader(
        spark=spark,
        source_subdir="scada_10min",
        target_table="scada_10min_raw",
    )
    ingest_stream_with_autoloader(
        spark=spark,
        source_subdir="status",
        target_table="status_events_raw",
    )

    print("All Bronze layer tables ingested successfully.")


if __name__ == "__main__":
    main()
