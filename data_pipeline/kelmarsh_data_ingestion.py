import hashlib
import re
import shutil
import sys
import zipfile
from pathlib import Path

import requests

ZENODO_RECORD_ID = "16807551"
ZENODO_API_URL = f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}"

# Unity Catalog External Volume path (maps to abfss://wind-platform@stwinddatacnbg.../landing/kelmarsh)
VOLUME_ROOT = Path("/Volumes/kelmarsh_wind/bronze/kelmarsh_landing_vol")
TMP_DIR = Path("/tmp/zenodo_kelmarsh")


def verify_md5(file_path: Path, expected_checksum: str) -> bool:
    """Verify MD5 checksum of a downloaded file against Zenodo metadata."""
    expected_hash = expected_checksum.replace("md5:", "").strip()
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(16 * 1024 * 1024), b""):
            md5.update(chunk)
    return md5.hexdigest() == expected_hash


def download_to_cloud_tmp(url: str, dest_path: Path, expected_checksum: str) -> None:
    """Download archive from Zenodo directly to Azure compute ephemeral storage."""
    print(f"Downloading {dest_path.name} on Azure compute...")
    with requests.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=16 * 1024 * 1024):
                if chunk:
                    f.write(chunk)

    if not verify_md5(dest_path, expected_checksum):
        dest_path.unlink(missing_ok=True)
        raise ValueError(f"MD5 checksum mismatch for {dest_path.name}")
    print(f"Verified MD5 checksum for {dest_path.name}")


def extract_year(filename: str) -> str:
    """Extract 4-digit year (2016-2024) from filename."""
    match = re.search(r"(201[6-9]|202[0-4])", filename)
    return match.group(1) if match else "unknown"


def extract_zip_to_volume(zip_path: Path) -> None:
    """Extract CSVs from cloud /tmp and stream directly into the ADLS Volume."""
    year = extract_year(zip_path.name)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            if member.is_dir() or not member.filename.lower().endswith(".csv"):
                continue

            base_name = Path(member.filename).name
            if base_name.startswith("Turbine_Data_Kelmarsh"):
                target_dir = VOLUME_ROOT / "scada_10min" / f"year={year}"
            elif base_name.startswith("Status_Kelmarsh"):
                target_dir = VOLUME_ROOT / "status" / f"year={year}"
            else:
                target_dir = VOLUME_ROOT / "other" / f"year={year}"

            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir / base_name

            if target_file.exists() and target_file.stat().st_size == member.file_size:
                print(f"Skipping existing file: {target_file}")
                continue

            print(f"Extracting to ADLS Volume: {target_file}")
            with zf.open(member, "r") as src, open(target_file, "wb") as dst:
                shutil.copyfileobj(src, dst, length=16 * 1024 * 1024)


def main() -> None:
    if not VOLUME_ROOT.exists():
        sys.exit(
            f"Error: Volume path {VOLUME_ROOT} not found. "
            "Ensure this script runs on Databricks compute with access to kelmarsh_wind.bronze.kelmarsh_landing_vol."
        )

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    year_arg = sys.argv[1] if len(sys.argv) > 1 else "ALL"
    selected_years = (
        []
        if year_arg.upper() == "ALL"
        else [y.strip() for y in year_arg.split(",") if y.strip()]
    )

    print(f"Fetching file manifest from Zenodo API ({ZENODO_API_URL})...")
    resp = requests.get(ZENODO_API_URL, timeout=30)
    resp.raise_for_status()
    files = resp.json().get("files", [])

    for file_info in files:
        fname = file_info["key"]
        download_url = file_info["links"]["self"]
        checksum = file_info["checksum"]
        fsize = file_info["size"]

        if fname.lower().endswith(".kmz"):
            continue

        # 1. Static Turbine Metadata CSV
        if fname.lower().endswith(".csv"):
            static_dir = VOLUME_ROOT / "static"
            static_dir.mkdir(parents=True, exist_ok=True)
            target_csv = static_dir / fname

            if target_csv.exists() and target_csv.stat().st_size == fsize:
                print(f"Skipping static file (already in ADLS): {target_csv}")
                continue

            tmp_csv = TMP_DIR / fname
            download_to_cloud_tmp(download_url, tmp_csv, checksum)
            shutil.move(str(tmp_csv), str(target_csv))
            print(f"Saved static file to ADLS Volume: {target_csv}")

        # 2. Yearly SCADA ZIP Archives (2016 - 2024)
        elif fname.lower().endswith(".zip") and "SCADA" in fname:
            year = extract_year(fname)
            if selected_years and year not in selected_years:
                continue

            tmp_zip = TMP_DIR / fname
            download_to_cloud_tmp(download_url, tmp_zip, checksum)
            extract_zip_to_volume(tmp_zip)
            tmp_zip.unlink(missing_ok=True)

    print("Cloud-to-cloud Kelmarsh 2016-2024 ingestion into ADLS Gen2 completed.")


if __name__ == "__main__":
    main()
