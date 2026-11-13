from pathlib import Path

from pyspark.sql import Column

from data_pipeline.kelmarsh_data_ingestion import extract_year
from databricks.bronze_ingestion import strip_kelmarsh_preamble
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
