from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess

# Constants
GCP_CONN_ID = "google_cloud_default"  # Update with your GCP connection ID
BUCKET_NAME = "your-gcs-bucket-name"
INPUT_FILE = "input/your_file.Z"  # Update with your actual file name
OUTPUT_DIR = "output/"
LOCAL_TEMP_DIR = "/tmp/uncompress_temp"

# Function to download a file from GCS
def download_z_file():
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_TEMP_DIR, os.path.basename(INPUT_FILE))
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    gcs_hook.download(bucket_name=BUCKET_NAME, object_name=INPUT_FILE, filename=local_path)
    return local_path

# Function to uncompress the .Z file
def uncompress_z_file():
    local_z_path = download_z_file()
    if not os.path.exists(local_z_path):
        raise FileNotFoundError(f"File not found: {local_z_path}")

    command = ["uncompress", local_z_path]
    try:
        subprocess.run(command, check=True)
        uncompressed_path = local_z_path.rstrip(".Z")
        return uncompressed_path
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error uncompressing file: {e}")

# Function to upload the uncompressed file back to GCS
def upload_uncompressed_file():
    uncompressed_path = uncompress_z_file()
    output_blob_name = OUTPUT_DIR + os.path.basename(uncompressed_path)
    
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    gcs_hook.upload(bucket_name=BUCKET_NAME, object_name=output_blob_name, filename=uncompressed_path)
    
    # Cleanup temporary files
    os.remove(uncompressed_path)

# Define the DAG
with DAG(
    "uncompress_single_z_file_dag",
    default_args={"start_date": datetime(2024, 1, 1), "retries": 1},
    schedule_interval=None,  # Trigger manually
    catchup=False,
) as dag:

    uncompress_and_upload = PythonOperator(
        task_id="uncompress_and_upload",
        python_callable=upload_uncompressed_file
    )

    uncompress_and_upload
