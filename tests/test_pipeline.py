from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import Column

from data_pipeline.kelmarsh_data_ingestion import extract_year
from databricks.bronze_ingestion import strip_kelmarsh_preamble
from databricks.ml_power_prediction import add_cyclical_features
from databricks.silver_transformation import clean_numeric_col


def test_extract_year_valid_and_unknown() -> None:
    """Verify year extraction from Kelmarsh Zenodo archive filenames."""
    assert extract_year("Kelmarsh_SCADA_2016_3086.zip") == "2016"
    assert extract_year("Kelmarsh_SCADA_2024_4509.zip") == "2024"
    assert extract_year("Kelmarsh_WT_static.csv") == "unknown"


def test_strip_kelmarsh_preamble_removes_comments(tmp_path: Path) -> None:
    """Verify 9-line '#' preamble is stripped while keeping '# Date and time' as header."""
    sample_csv = tmp_path / "Turbine_Data_Kelmarsh_1_2022.csv"
    raw_content = (
        "# Site: Kelmarsh wind farm\n"
        "# Turbine: Kelmarsh 1\n"
        "# Exported: 2023-01-01\n"
        "# Date and time,Wind speed (m/s),Power (kW)\n"
        "2022-01-01 00:00:00,8.5,1250.4\n"
        "2022-01-01 00:10:00,NaN,-\n"
    )
    sample_csv.write_text(raw_content, encoding="utf-8")

    strip_kelmarsh_preamble(str(tmp_path))

    cleaned_lines = sample_csv.read_text(encoding="utf-8").splitlines()
    assert len(cleaned_lines) == 3
    assert cleaned_lines[0] == "Date and time,Wind speed (m/s),Power (kW)"
    assert cleaned_lines[1] == "2022-01-01 00:00:00,8.5,1250.4"


def test_strip_kelmarsh_preamble_is_idempotent(tmp_path: Path) -> None:
    """Verify running strip_kelmarsh_preamble twice does not modify an already clean CSV."""
    sample_csv = tmp_path / "Status_Kelmarsh_1_2022.csv"
    raw_content = (
        "# Comment line 1\n"
        "# Timestamp start,Timestamp end,Status\n"
        "2022-01-01 00:00:00,-,Stop\n"
    )
    sample_csv.write_text(raw_content, encoding="utf-8")

    strip_kelmarsh_preamble(str(tmp_path))
    first_pass = sample_csv.read_text(encoding="utf-8")

    strip_kelmarsh_preamble(str(tmp_path))
    second_pass = sample_csv.read_text(encoding="utf-8")

    assert first_pass == second_pass
    assert first_pass.startswith("Timestamp start,Timestamp end,Status\n")


def test_clean_numeric_col_returns_spark_column() -> None:
    """Verify clean_numeric_col builds a valid PySpark Column expression."""
    col_expr = clean_numeric_col("Wind speed (m/s)", "double")
    assert isinstance(col_expr, Column)
    expr_str = str(col_expr)
    assert "Wind speed (m/s)" in expr_str
    assert "nan" in expr_str.lower()


def test_add_cyclical_features_computes_periodic_bounds() -> None:
    """Verify wind direction (0 vs 360 deg) and month cyclical features align."""
    df = pd.DataFrame(
        {
            "wind_direction_deg": [0.0, 90.0, 360.0],
            "event_month": [1, 4, 1],
        }
    )
    enriched = add_cyclical_features(df)

    # 0 deg and 360 deg must map to identical sin/cos coordinates
    assert np.isclose(enriched.loc[0, "wind_dir_sin"], enriched.loc[2, "wind_dir_sin"])
    assert np.isclose(enriched.loc[0, "wind_dir_cos"], enriched.loc[2, "wind_dir_cos"])
    assert np.isclose(enriched.loc[1, "wind_dir_sin"], 1.0)
    assert np.isclose(enriched.loc[1, "wind_dir_cos"], 0.0)

    # Month 1 (Jan) starts at angle 0 -> sin=0, cos=1; Month 4 (Apr) -> 90 deg -> sin=1, cos=0
    assert np.isclose(enriched.loc[0, "month_sin"], 0.0)
    assert np.isclose(enriched.loc[0, "month_cos"], 1.0)
    assert np.isclose(enriched.loc[1, "month_sin"], 1.0)
    assert np.isclose(enriched.loc[1, "month_cos"], 0.0)
