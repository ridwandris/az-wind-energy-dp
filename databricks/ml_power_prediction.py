import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.models.signature import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

CATALOG = "kelmarsh_wind"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
REGISTERED_MODEL_NAME = f"{CATALOG}.{GOLD_SCHEMA}.turbine_power_digital_twin"

FEATURE_COLS = [
    "turbine_id",
    "wind_speed_ms",
    "wind_speed_std_ms",
    "wind_speed_max_ms",
    "density_adj_wind_speed_ms",
    "ambient_temp_c",
    "nacelle_temp_c",
    "wind_dir_sin",
    "wind_dir_cos",
    "month_sin",
    "month_cos",
]
TARGET_COL = "active_power_kw"


def add_cyclical_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Add cyclical sine/cosine transformations for wind direction (0-360 deg)
    and calendar month (1-12) so gradient boosted trees capture periodic boundaries.
    """
    out = pdf.copy()
    wind_rad = np.deg2rad(out["wind_direction_deg"].astype(float))
    out["wind_dir_sin"] = np.sin(wind_rad)
    out["wind_dir_cos"] = np.cos(wind_rad)

    month_rad = 2.0 * np.pi * (out["event_month"].astype(float) - 1.0) / 12.0
    out["month_sin"] = np.sin(month_rad)
    out["month_cos"] = np.cos(month_rad)
    return out


def load_scada_dataframe(spark: SparkSession) -> pd.DataFrame:
    """Load valid 10-minute SCADA telemetry from silver.scada_10min_clean into Pandas."""
    scada_table = f"{CATALOG}.{SILVER_SCHEMA}.scada_10min_clean"
    print(f"Loading valid SCADA telemetry from {scada_table}...")

    df_spark = (
        spark.table(scada_table)
        .filter(
            (F.col("is_valid_reading") == F.lit(True))
            & F.col("wind_speed_ms").isNotNull()
            & F.col("active_power_kw").isNotNull()
            & F.col("wind_direction_deg").isNotNull()
        )
        .select(
            "turbine_id",
            "event_timestamp",
            "event_date",
            "event_year",
            "event_month",
            "wind_speed_ms",
            "wind_speed_std_ms",
            "wind_speed_max_ms",
            "density_adj_wind_speed_ms",
            "wind_direction_deg",
            "ambient_temp_c",
            "nacelle_temp_c",
            "active_power_kw",
            "potential_power_kw",
            F.coalesce(F.col("lost_prod_downtime_kwh"), F.lit(0.0)).alias(
                "lost_prod_downtime_kwh"
            ),
            F.coalesce(F.col("lost_prod_curtailment_kwh"), F.lit(0.0)).alias(
                "lost_prod_curtailment_kwh"
            ),
        )
    )

    pdf = df_spark.toPandas()
    print(f"Loaded {len(pdf):,} valid 10-minute SCADA rows.")
    return add_cyclical_features(pdf)


def train_and_register_digital_twin(
    pdf: pd.DataFrame,
) -> HistGradientBoostingRegressor:
    """
    Train HistGradientBoostingRegressor on healthy 2016-2022 data, validate on
    healthy 2023 data, log metrics/signature to MLflow, and register in Unity Catalog.
    """
    healthy_mask = (pdf["lost_prod_downtime_kwh"] <= 0.0) & (
        pdf["lost_prod_curtailment_kwh"] <= 0.0
    )

    train_df = pdf[healthy_mask & (pdf["event_year"].between(2016, 2022))].copy()
    val_df = pdf[healthy_mask & (pdf["event_year"] == 2023)].copy()

    if val_df.empty:
        # Fallback split if 2023 healthy rows are unavailable
        val_df = train_df.sample(frac=0.15, random_state=42)
        train_df = train_df.drop(val_df.index)

    X_train = train_df[FEATURE_COLS].astype(float)
    y_train = train_df[TARGET_COL].astype(float)
    X_val = val_df[FEATURE_COLS].astype(float)
    y_val = val_df[TARGET_COL].astype(float)

    print(
        f"Training Digital Twin on {len(X_train):,} healthy rows (2016-2022); "
        f"validating on {len(X_val):,} rows (2023)..."
    )

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment("/Users/riddris009@outlook.com/kelmarsh_power_digital_twin")

    model = HistGradientBoostingRegressor(
        max_iter=250,
        learning_rate=0.06,
        max_leaf_nodes=63,
        min_samples_leaf=30,
        l2_regularization=1.0,
        random_state=42,
    )

    with mlflow.start_run(run_name="kelmarsh_power_digital_twin_hgb"):
        model.fit(X_train, y_train)

        val_preds = np.clip(model.predict(X_val), 0.0, 2050.0)
        rmse = float(np.sqrt(mean_squared_error(y_val, val_preds)))
        mae = float(mean_absolute_error(y_val, val_preds))
        r2 = float(r2_score(y_val, val_preds))

        print(
            f"Validation Metrics (2023) -> RMSE: {rmse:.2f} kW | MAE: {mae:.2f} kW | R2: {r2:.4f}"
        )

        mlflow.log_params(
            {
                "model_type": "HistGradientBoostingRegressor",
                "train_years": "2016-2022",
                "validation_year": "2023",
                "max_iter": 250,
                "learning_rate": 0.06,
                "max_leaf_nodes": 63,
                "min_samples_leaf": 30,
                "train_rows": len(X_train),
                "val_rows": len(X_val),
            }
        )
        mlflow.log_metrics(
            {
                "val_rmse_kw": round(rmse, 3),
                "val_mae_kw": round(mae, 3),
                "val_r2_score": round(r2, 4),
            }
        )

        sample_input = X_train.head(50)
        signature = infer_signature(sample_input, model.predict(sample_input))

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="digital_twin_model",
            signature=signature,
            input_example=X_train.head(5),
            registered_model_name=REGISTERED_MODEL_NAME,
        )
        print(
            f"Registered Digital Twin model in Unity Catalog: {REGISTERED_MODEL_NAME}"
        )

    return model


def score_and_save_anomalies(
    spark: SparkSession,
    pdf: pd.DataFrame,
    model: HistGradientBoostingRegressor,
) -> None:
    """
    Score 2023-2024 data, flag underperforming intervals (wind speed 5-20 m/s,
    no logged downtime/curtailment, predicted - actual > 250 kW), and write daily
    anomaly summaries to kelmarsh_wind.gold.ml_turbine_anomalies.
    """
    target_table = f"{CATALOG}.{GOLD_SCHEMA}.ml_turbine_anomalies"
    dim_table = f"{CATALOG}.{SILVER_SCHEMA}.turbines_dim"

    score_df = pdf[pdf["event_year"].isin([2023, 2024])].copy()
    if score_df.empty:
        score_df = pdf.copy()

    print(f"Scoring {len(score_df):,} intervals for 2023-2024 anomaly detection...")
    X_score = score_df[FEATURE_COLS].astype(float)
    score_df["predicted_power_kw"] = np.clip(model.predict(X_score), 0.0, 2050.0)
    score_df["power_residual_kw"] = (
        score_df["predicted_power_kw"] - score_df["active_power_kw"]
    )

    score_df["is_underperforming_anomaly"] = (
        score_df["wind_speed_ms"].between(5.0, 20.0)
        & (score_df["lost_prod_downtime_kwh"] <= 0.0)
        & (score_df["lost_prod_curtailment_kwh"] <= 0.0)
        & (score_df["power_residual_kw"] > 250.0)
    )

    score_df["unexplained_loss_kwh"] = np.where(
        score_df["is_underperforming_anomaly"],
        score_df["power_residual_kw"] / 6.0,
        0.0,
    )

    daily_summary = (
        score_df.groupby(
            ["turbine_id", "event_date", "event_year", "event_month"], as_index=False
        )
        .agg(
            evaluated_intervals=("active_power_kw", "count"),
            anomaly_intervals=("is_underperforming_anomaly", "sum"),
            avg_wind_speed_ms=("wind_speed_ms", "mean"),
            avg_actual_power_kw=("active_power_kw", "mean"),
            avg_predicted_power_kw=("predicted_power_kw", "mean"),
            avg_power_residual_kw=("power_residual_kw", "mean"),
            max_power_residual_kw=("power_residual_kw", "max"),
            estimated_unexplained_loss_kwh=("unexplained_loss_kwh", "sum"),
        )
        .assign(
            anomaly_rate_pct=lambda d: np.round(
                (d["anomaly_intervals"] / d["evaluated_intervals"]) * 100.0, 2
            ),
            avg_wind_speed_ms=lambda d: np.round(d["avg_wind_speed_ms"], 2),
            avg_actual_power_kw=lambda d: np.round(d["avg_actual_power_kw"], 2),
            avg_predicted_power_kw=lambda d: np.round(d["avg_predicted_power_kw"], 2),
            avg_power_residual_kw=lambda d: np.round(d["avg_power_residual_kw"], 2),
            max_power_residual_kw=lambda d: np.round(d["max_power_residual_kw"], 2),
            estimated_unexplained_loss_kwh=lambda d: np.round(
                d["estimated_unexplained_loss_kwh"], 2
            ),
        )
    )

    df_anomalies_spark = spark.createDataFrame(daily_summary)
    df_dim = spark.table(dim_table).select("turbine_id", "turbine_name", "model")

    df_gold_anomalies = (
        df_anomalies_spark.withColumn("turbine_id", F.col("turbine_id").cast("int"))
        .withColumn("event_date", F.to_date("event_date"))
        .withColumn("event_year", F.col("event_year").cast("int"))
        .withColumn("event_month", F.col("event_month").cast("int"))
        .withColumn("evaluated_intervals", F.col("evaluated_intervals").cast("int"))
        .withColumn("anomaly_intervals", F.col("anomaly_intervals").cast("int"))
        .join(df_dim, on="turbine_id", how="left")
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        df_gold_anomalies.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .clusterBy("turbine_id", "event_date")
        .saveAsTable(target_table)
    )
    row_count = spark.table(target_table).count()
    print(f"Saved {target_table} ({row_count} daily rows).")


def main() -> None:
    spark = SparkSession.builder.appName("Kelmarsh_ML_Digital_Twin").getOrCreate()

    pdf = load_scada_dataframe(spark)
    model = train_and_register_digital_twin(pdf)
    score_and_save_anomalies(spark, pdf, model)

    print("ML Digital Twin training, registration, and anomaly scoring completed.")


if __name__ == "__main__":
    main()
