from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "kelmarsh_wind"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


def build_daily_turbine_performance(spark: SparkSession) -> None:
    """
    Build gold.daily_turbine_performance by joining cleaned 10-minute SCADA data
    with static turbine specifications to compute daily yield, Capacity Factor,
    and Power Performance Index.
    """
    scada_table = f"{CATALOG}.{SILVER_SCHEMA}.scada_10min_clean"
    dim_table = f"{CATALOG}.{SILVER_SCHEMA}.turbines_dim"
    target_table = f"{CATALOG}.{GOLD_SCHEMA}.daily_turbine_performance"

    print(f"Building {target_table}...")
    df_scada = spark.table(scada_table).filter(F.col("is_valid_reading") == F.lit(True))
    df_dim = spark.table(dim_table)

    df_daily = df_scada.groupBy(
        "turbine_id", "event_date", "event_year", "event_month"
    ).agg(
        F.count("*").alias("valid_10min_intervals"),
        F.round(F.avg("wind_speed_ms"), 2).alias("avg_wind_speed_ms"),
        F.round(F.max("wind_speed_max_ms"), 2).alias("peak_wind_speed_ms"),
        F.round(F.avg("active_power_kw"), 2).alias("avg_active_power_kw"),
        # Convert 10-minute average kW to kWh (kW * (10/60) hours) if energy_export_kwh is null
        F.round(
            F.sum(
                F.coalesce(
                    F.col("energy_export_kwh"),
                    F.when(
                        F.col("active_power_kw") > 0,
                        F.col("active_power_kw") / 6.0,
                    ).otherwise(0.0),
                )
            ),
            2,
        ).alias("actual_energy_kwh"),
        F.round(
            F.sum(
                F.when(
                    F.col("potential_power_kw") > 0,
                    F.col("potential_power_kw") / 6.0,
                ).otherwise(0.0)
            ),
            2,
        ).alias("potential_energy_kwh"),
        F.round(
            F.sum(F.coalesce(F.col("lost_prod_downtime_kwh"), F.lit(0.0))), 2
        ).alias("lost_downtime_kwh"),
        F.round(
            F.sum(F.coalesce(F.col("lost_prod_curtailment_kwh"), F.lit(0.0))), 2
        ).alias("lost_curtailment_kwh"),
        F.round(F.avg("nacelle_temp_c"), 2).alias("avg_nacelle_temp_c"),
        F.round(F.avg("gear_oil_temp_c"), 2).alias("avg_gear_oil_temp_c"),
        F.round(F.avg("data_availability_ratio") * 100.0, 2).alias(
            "data_availability_pct"
        ),
    )

    df_gold = (
        df_daily.join(
            df_dim.select(
                "turbine_id",
                "turbine_name",
                "manufacturer",
                "model",
                "rated_power_kw",
            ),
            on="turbine_id",
            how="left",
        )
        .withColumn(
            "theoretical_max_daily_kwh",
            F.col("rated_power_kw") * F.lit(24.0),
        )
        .withColumn(
            "capacity_factor_pct",
            F.round(
                (F.col("actual_energy_kwh") / F.col("theoretical_max_daily_kwh"))
                * 100.0,
                2,
            ),
        )
        .withColumn(
            "power_performance_index_pct",
            F.when(
                F.col("potential_energy_kwh") > 0,
                F.round(
                    (F.col("actual_energy_kwh") / F.col("potential_energy_kwh"))
                    * 100.0,
                    2,
                ),
            ).otherwise(F.lit(None)),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        df_gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("turbine_id", "event_date")
        .saveAsTable(target_table)
    )
    print(f"Saved {target_table} ({spark.table(target_table).count()} rows).")


def build_iec_power_curves(spark: SparkSession) -> None:
    """
    Build gold.iec_power_curves using standard IEC 61400-12-1 0.5 m/s wind speed bins.
    Allows comparing actual vs. reference turbine power curves across years.
    """
    scada_table = f"{CATALOG}.{SILVER_SCHEMA}.scada_10min_clean"
    dim_table = f"{CATALOG}.{SILVER_SCHEMA}.turbines_dim"
    target_table = f"{CATALOG}.{GOLD_SCHEMA}.iec_power_curves"

    print(f"Building {target_table}...")
    df_scada = (
        spark.table(scada_table)
        .filter(
            (F.col("is_valid_reading") == F.lit(True))
            & F.col("wind_speed_ms").isNotNull()
            & F.col("active_power_kw").isNotNull()
        )
        # Bin wind speed to nearest 0.5 m/s increment: round(ws * 2) / 2
        .withColumn(
            "wind_speed_bin_ms",
            F.round(F.col("wind_speed_ms") * 2.0, 0) / 2.0,
        )
    )

    df_dim = spark.table(dim_table).select(
        "turbine_id", "turbine_name", "rated_power_kw"
    )

    df_curves = (
        df_scada.groupBy("turbine_id", "event_year", "wind_speed_bin_ms")
        .agg(
            F.count("*").alias("interval_count"),
            F.round(F.avg("wind_speed_ms"), 3).alias("mean_wind_speed_ms"),
            F.round(F.avg("active_power_kw"), 2).alias("avg_active_power_kw"),
            F.round(F.stddev("active_power_kw"), 2).alias("std_active_power_kw"),
            F.round(F.avg("potential_power_kw"), 2).alias("avg_potential_power_kw"),
            F.round(F.avg("rotor_speed_rpm"), 2).alias("avg_rotor_speed_rpm"),
            F.round(F.avg("pitch_angle_a_deg"), 2).alias("avg_pitch_angle_deg"),
        )
        .withColumn(
            "power_deficit_kw",
            F.round(F.col("avg_potential_power_kw") - F.col("avg_active_power_kw"), 2),
        )
        .join(df_dim, on="turbine_id", how="left")
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        df_curves.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("turbine_id", "event_year")
        .saveAsTable(target_table)
    )
    print(f"Saved {target_table} ({spark.table(target_table).count()} rows).")


def build_monthly_fault_summary(spark: SparkSession) -> None:
    """
    Build gold.monthly_fault_summary from silver.status_events_clean to analyze
    turbine downtime hours, fault frequencies, and Mean Time To Resolve (MTTR).
    """
    status_table = f"{CATALOG}.{SILVER_SCHEMA}.status_events_clean"
    dim_table = f"{CATALOG}.{SILVER_SCHEMA}.turbines_dim"
    target_table = f"{CATALOG}.{GOLD_SCHEMA}.monthly_fault_summary"

    print(f"Building {target_table}...")
    df_status = (
        spark.table(status_table)
        .filter(
            F.col("duration_seconds").isNotNull() & (F.col("duration_seconds") >= 0)
        )
        .withColumn("event_month", F.month("start_timestamp"))
    )

    df_dim = spark.table(dim_table).select("turbine_id", "turbine_name")

    df_faults = (
        df_status.groupBy(
            "turbine_id",
            "event_year",
            "event_month",
            "status_type",
            "iec_category",
        )
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.sum("duration_seconds") / 3600.0, 2).alias(
                "total_duration_hours"
            ),
            F.round(F.avg("duration_seconds") / 3600.0, 2).alias("mttr_hours"),
        )
        .join(df_dim, on="turbine_id", how="left")
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        df_faults.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("turbine_id", "event_year")
        .saveAsTable(target_table)
    )
    print(f"Saved {target_table} ({spark.table(target_table).count()} rows).")


def main() -> None:
    spark = SparkSession.builder.appName("Kelmarsh_Gold_Aggregations").getOrCreate()

    build_daily_turbine_performance(spark)
    build_iec_power_curves(spark)
    build_monthly_fault_summary(spark)

    print("All Gold layer KPI tables built successfully.")


if __name__ == "__main__":
    main()
