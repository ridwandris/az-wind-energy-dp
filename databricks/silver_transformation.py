from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

CATALOG = "kelmarsh_wind"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"


def clean_numeric_col(col_name: str, target_type: str = "double") -> F.Column:
    """
    Safely cast raw string/numeric columns that may contain literal 'NaN' or '-'
    strings into proper SQL NULLs and target numeric types.
    """
    raw_col = F.col(f"`{col_name}`").cast("string")
    return (
        F.when(
            raw_col.isNull()
            | (F.trim(raw_col) == "")
            | (F.lower(F.trim(raw_col)) == "nan")
            | (F.trim(raw_col) == "-"),
            F.lit(None),
        )
        .otherwise(raw_col)
        .cast(target_type)
    )


def build_turbines_dim(spark: SparkSession) -> None:
    """Transform bronze.turbine_static_raw into silver.turbines_dim."""
    source_table = f"{CATALOG}.{BRONZE_SCHEMA}.turbine_static_raw"
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.turbines_dim"

    print(f"Building {target_table} from {source_table}...")
    df_raw = spark.table(source_table)

    # Extract integer turbine_id from Title (e.g. 'Kelmarsh 1' -> 1)
    df_dim = (
        df_raw.select(
            F.regexp_extract(F.col("`Title`").cast("string"), r"(\d+)", 1)
            .cast("int")
            .alias("turbine_id"),
            F.col("`Title`").cast("string").alias("turbine_name"),
            F.col("`Manufacturer`").cast("string").alias("manufacturer"),
            F.col("`Model`").cast("string").alias("model"),
            clean_numeric_col("Rated power (kW)", "double").alias("rated_power_kw"),
            clean_numeric_col("Rotor diameter (m)", "double").alias("rotor_diameter_m"),
            clean_numeric_col("Hub height (m)", "double").alias("hub_height_m"),
            clean_numeric_col("Latitude", "double").alias("latitude"),
            clean_numeric_col("Longitude", "double").alias("longitude"),
            F.current_timestamp().alias("processed_at"),
        )
        .filter(F.col("turbine_id").isNotNull())
        .dropDuplicates(["turbine_id"])
    )

    (
        df_dim.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    print(f"Saved {target_table} ({df_dim.count()} rows).")


def build_scada_10min_clean(spark: SparkSession) -> None:
    """
    Transform bronze.scada_10min_raw into silver.scada_10min_clean.
    Includes standardization, deduplication, physical range validation, and Liquid Clustering.
    """
    source_table = f"{CATALOG}.{BRONZE_SCHEMA}.scada_10min_raw"
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.scada_10min_clean"

    print(f"Building {target_table} from {source_table}...")
    df_raw = spark.table(source_table)

    df_selected = df_raw.select(
        F.col("turbine_id").cast("int").alias("turbine_id"),
        F.to_timestamp(F.col("`Date and time`").cast("string")).alias(
            "event_timestamp"
        ),
        clean_numeric_col("Wind speed (m/s)").alias("wind_speed_ms"),
        clean_numeric_col("Wind speed, Standard deviation (m/s)").alias(
            "wind_speed_std_ms"
        ),
        clean_numeric_col("Wind speed, Maximum (m/s)").alias("wind_speed_max_ms"),
        clean_numeric_col("Density adjusted wind speed (m/s)").alias(
            "density_adj_wind_speed_ms"
        ),
        clean_numeric_col("Wind direction (°)").alias("wind_direction_deg"),
        clean_numeric_col("Nacelle position (°)").alias("nacelle_position_deg"),
        clean_numeric_col("Power (kW)").alias("active_power_kw"),
        clean_numeric_col("Potential power default PC (kW)").alias(
            "potential_power_kw"
        ),
        clean_numeric_col("Reactive power (kvar)").alias("reactive_power_kvar"),
        clean_numeric_col("Rotor speed (RPM)").alias("rotor_speed_rpm"),
        clean_numeric_col("Generator RPM (RPM)").alias("generator_rpm"),
        clean_numeric_col("Blade angle (pitch position) A (°)").alias(
            "pitch_angle_a_deg"
        ),
        clean_numeric_col("Nacelle ambient temperature (°C)").alias("ambient_temp_c"),
        clean_numeric_col("Nacelle temperature (°C)").alias("nacelle_temp_c"),
        clean_numeric_col("Gear oil temperature (°C)").alias("gear_oil_temp_c"),
        clean_numeric_col("Generator bearing front temperature (°C)").alias(
            "gen_bearing_front_temp_c"
        ),
        clean_numeric_col("Generator bearing rear temperature (°C)").alias(
            "gen_bearing_rear_temp_c"
        ),
        clean_numeric_col("Energy Export (kWh)").alias("energy_export_kwh"),
        clean_numeric_col("Lost Production to Downtime (kWh)").alias(
            "lost_prod_downtime_kwh"
        ),
        clean_numeric_col("Lost Production to Curtailment (Total) (kWh)").alias(
            "lost_prod_curtailment_kwh"
        ),
        clean_numeric_col("Data Availability").alias("data_availability_ratio"),
        F.col("source_file"),
        F.col("ingested_at"),
    ).filter(F.col("turbine_id").isNotNull() & F.col("event_timestamp").isNotNull())

    # Deduplicate on (turbine_id, event_timestamp) keeping the latest ingested file
    dedup_window = Window.partitionBy("turbine_id", "event_timestamp").orderBy(
        F.col("ingested_at").desc(), F.col("source_file").desc()
    )

    df_deduped: DataFrame = (
        df_selected.withColumn("row_num", F.row_number().over(dedup_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Add date/time attributes and Data Quality flag for physical sensor boundaries
    # Senvion MM92 rated power is 2050 kW (allow small transient spikes up to 2500 kW)
    df_enriched = (
        df_deduped.withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_year", F.year("event_timestamp"))
        .withColumn("event_month", F.month("event_timestamp"))
        .withColumn(
            "is_valid_reading",
            F.when(
                F.col("wind_speed_ms").between(0.0, 50.0)
                & F.col("active_power_kw").between(-150.0, 2500.0)
                & F.col("wind_direction_deg").between(0.0, 360.0),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        df_enriched.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("turbine_id", "event_date")
        .saveAsTable(target_table)
    )

    total_rows = spark.table(target_table).count()
    print(f"Saved {target_table} with Liquid Clustering ({total_rows} rows).")


def build_status_events_clean(spark: SparkSession) -> None:
    """Transform bronze.status_events_raw into silver.status_events_clean."""
    source_table = f"{CATALOG}.{BRONZE_SCHEMA}.status_events_raw"
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.status_events_clean"

    print(f"Building {target_table} from {source_table}...")
    df_raw = spark.table(source_table)

    # Status files have 'Timestamp start', 'Timestamp end', 'Duration', 'Status', 'Code', 'Message', 'IEC category'
    df_selected = df_raw.select(
        F.col("turbine_id").cast("int").alias("turbine_id"),
        F.to_timestamp(F.col("`Timestamp start`").cast("string")).alias(
            "start_timestamp"
        ),
        F.try_to_timestamp(F.col("`Timestamp end`").cast("string")).alias(
            "end_timestamp"
        ),
        F.col("`Status`").cast("string").alias("status_type"),
        clean_numeric_col("Code", "int").alias("status_code"),
        F.col("`Message`").cast("string").alias("status_message"),
        F.col("`IEC category`").cast("string").alias("iec_category"),
        F.col("source_file"),
        F.col("ingested_at"),
    ).filter(F.col("turbine_id").isNotNull() & F.col("start_timestamp").isNotNull())

    dedup_window = Window.partitionBy(
        "turbine_id", "start_timestamp", "status_code", "status_message"
    ).orderBy(F.col("ingested_at").desc())

    df_clean = (
        df_selected.withColumn("row_num", F.row_number().over(dedup_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn(
            "duration_seconds",
            F.when(
                F.col("end_timestamp").isNotNull(),
                F.unix_timestamp("end_timestamp") - F.unix_timestamp("start_timestamp"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("event_date", F.to_date("start_timestamp"))
        .withColumn("event_year", F.year("start_timestamp"))
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        df_clean.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("turbine_id", "event_date")
        .saveAsTable(target_table)
    )

    total_rows = spark.table(target_table).count()
    print(f"Saved {target_table} ({total_rows} rows).")


def main() -> None:
    spark = SparkSession.builder.appName("Kelmarsh_Silver_Transformation").getOrCreate()

    build_turbines_dim(spark)
    build_scada_10min_clean(spark)
    build_status_events_clean(spark)

    print("All Silver layer tables built successfully.")


if __name__ == "__main__":
    main()
