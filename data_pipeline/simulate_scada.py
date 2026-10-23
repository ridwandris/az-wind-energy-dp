import json
import os
import random
import sys
import time
from datetime import UTC, datetime

from azure.core.exceptions import HttpResponseError
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER")

if not STORAGE_ACCOUNT_NAME or not CONTAINER_NAME:
    sys.exit(
        "Error: Missing required environment variables. "
        "Ensure AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_CONTAINER are set in .env."
    )

ACCOUNT_URL = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"


def generate_turbine_reading(turbine_id: str) -> dict:
    """Generate realistic SCADA operational metrics for a single wind turbine."""
    wind_speed = round(random.uniform(3.0, 22.0), 2)
    rotor_speed = round(wind_speed * 0.8 + random.uniform(-0.5, 0.5), 2)
    active_power = round(wind_speed**3 * 0.25, 2) if wind_speed > 3.5 else 0.0
    pitch_angle = round(max(0.0, (wind_speed - 12.0) * 1.8), 2)

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "turbine_id": turbine_id,
        "wind_speed_ms": wind_speed,
        "rotor_speed_rpm": rotor_speed,
        "active_power_kw": active_power,
        "pitch_angle_deg": pitch_angle,
    }


def main():
    print(f"Authenticating via Azure CLI to {ACCOUNT_URL}...")
    try:
        credential = AzureCliCredential()
        credential.get_token("https://storage.azure.com/.default")
        print(
            f"Azure authentication successful for storage account {STORAGE_ACCOUNT_NAME}."
        )
    except Exception as exc:
        print(
            "Authentication failed. Please run 'az login' in the terminal and ensure your account has access to this storage account."
        )
        print(
            "Also grant a role like 'Storage Blob Data Contributor' on the storage account or container."
        )
        raise SystemExit(f"Azure CLI credential error: {exc}") from exc

    blob_service_client = BlobServiceClient(
        account_url=ACCOUNT_URL, credential=credential
    )
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    turbine_ids = ["WTG-01", "WTG-02", "WTG-03"]

    print("Beginning SCADA batch transmission...")
    for batch_id in range(5):
        records = [generate_turbine_reading(tid) for tid in turbine_ids]
        file_name = f"scada_stream/batch_{int(time.time())}_{batch_id}.json"
        payload = json.dumps(records, indent=2)

        blob_client = container_client.get_blob_client(file_name)
        try:
            blob_client.upload_blob(payload, overwrite=True)
            print(f"Uploaded: {file_name}")
        except HttpResponseError as exc:
            print(
                "Upload failed because the current Azure identity does not have permission to write to this Blob container."
            )
            print(
                "Fix: run 'az login' and ensure your user/service principal has 'Storage Blob Data Contributor' on the storage account or container."
            )
            print(f"Storage account: {STORAGE_ACCOUNT_NAME}")
            print(f"Container: {CONTAINER_NAME}")
            print(f"Azure error: {exc}")
            raise

        time.sleep(2)

    print("Batch generation completed successfully.")


if __name__ == "__main__":
    main()
