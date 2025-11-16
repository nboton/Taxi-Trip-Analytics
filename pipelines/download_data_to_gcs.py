import requests
from google.cloud import storage
import logging
import io
import time
from datetime import datetime
from datetime import UTC  # Import UTC explicitly

# Project ID, Bucket name, dataset
PROJECT_ID = "taxi-trip-analytics-478020"
BUCKET_NAME = "taxi-trip-analytics-bucket"
GCS_FOLDER = "dataset/trips/"


# Initialize Google Cloud Storage client
storage_client = storage.Client()

#Configure logging to capture INFO logs in an in-memory stream and send them to gcp
log_stream_memo = io.StringIO()
logging.basicConfig(stream=log_stream_memo, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def upload_log_to_gcs():
    """
    Upload the in-memory application logs to a Google Cloud Storage bucket.

    The log file is stored under the `log/` folder in the bucket, using a
    timestamped filename such as:
        log/extract_log_YYYYMMDD_HHMMSS.log

    Returns:
        None
    """
    log_file_name = f"log/extract_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    client = storage_client.bucket(BUCKET_NAME)
    blob = client.blob(log_file_name)
    blob.upload_from_string(log_stream_memo.getvalue())
    logging.info(f"Log file uploaded to {log_file_name}")


def file_exists_in_gcs(bucket_name, gcs_path):
    """
    Check whether a file exists in a Google Cloud Storage bucket.

    Args:
        bucket_name (str): Name of the GCS bucket.
        file_path (str): Full path to the file inside the bucket
                        (e.g., "trips/yellow_tripdata_2020-01.parquet").

    Returns:
        bool: True if the file exists, False otherwise.
    """    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob.exists()

def upload_to_gcs(bucket_name, gcs_path: str, data):
    """Upload bytes data to a GCS bucket path."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(data)
    

def download_historical_data_to_gcs():
    """
    Download historical NYC yellow taxi Parquet files (from 2019 up to the
    current year) and upload them directly to Google Cloud Storage.

    For each year and month:
        - Check if the file already exists in the GCS bucket.
        - If not, download it from the public source.
        - Upload the file to GCS under the path defined by GCS_FOLDER.
        - Log all steps, including missing files or download errors.

    Returns:
        None
    """
    
    try:
        for year in range(2019, datetime.now().year+1):
            for month in range(1, 13):
                file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
                gcs_path = f"{GCS_FOLDER}{file_name}"
                
                if file_exists_in_gcs(BUCKET_NAME, gcs_path):
                    logging.info(f"{file_name} already exists in GCS, skipping...")
                    continue

                try:
                    logging.info(f"Downloading {file_name}...")
                    response = requests.get(download_url, stream=True)

                    if response.status_code == 200:
                        upload_to_gcs(BUCKET_NAME, gcs_path, response.content)
                    elif response.status_code == 404:
                        logging.warning(f"File {file_name} not found on source, skipping...")
                    else:
                        logging.error(f"Failed to download {file_name}. HTTP status code: {response.status_code}")
                except Exception as e:
                    logging.error(f"Error downloading {file_name}: {str(e)}")

                time.sleep(1)
        logging.info("Download and upload to GCS completed!")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        upload_log_to_gcs()


if __name__ == '__main__':
    logging.info(f"Date the historical data was downloaded: {datetime.today()}")
    download_historical_data_to_gcs()
