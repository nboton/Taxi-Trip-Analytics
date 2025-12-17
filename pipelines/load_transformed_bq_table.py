# %%
import logging
from google.cloud import bigquery, storage
from datetime import datetime
import io
from datetime import UTC  # Import UTC explicitly

# Set project-specific variables
PROJECT_ID = "taxi-trip-analytics-478020"
BUCKET_NAME = "taxi-trip-analytics-bucket"
GCS_FOLDER = "dataset/trips/"
GCS_LOG_FOLDER = "log/"
TABLE_ID = f"{PROJECT_ID}.dataset_raw_taxitrips.trips"
TABLE_TRANSFORMED_ID = f"{PROJECT_ID}.transformed_data.clean_filtered_data_trips"
TEMP_TABLE_ID = f"{TABLE_ID}_temp" # Temporary table to load data without type constraints

# Initialize BigQuery and GCS clients
bq_client = bigquery.Client(project=PROJECT_ID, location="US")
storage_client = storage.Client()

#Configure logging to capture INFO logs in an in-memory stream and send them to gcp
log_stream_memo = io.StringIO()
logging.basicConfig(stream=log_stream_memo, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def upload_log_to_gcs():
    """
    Upload the in-memory application logs to a Google Cloud Storage bucket.

    The log file is stored under the `log/` folder in the bucket, using a
    timestamped filename such as:
        log/load_log_gcsfile_to_bq_YYYYMMDD_HHMMSS.log

    Returns:
        None
    """
   
    log_filename = f"{GCS_LOG_FOLDER}load_data_from_trpis_to_trips_transformed_table{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream_memo.getvalue())
    logging.info(f"Log file uploaded to {log_filename}")


def trips_bq_to_trips_transformed_bq():
    """
    Retrieve from raw data in trips table to trips_transformed table.
    """
    query = f"""
        
        CREATE OR REPLACE TABLE `{TABLE_TRANSFORMED_ID}`AS
        SELECT *
        FROM `{TABLE_ID}`
        WHERE total_amount>0 and payment_type!=6 and trip_distance>0 and passenger_count>0
    """
    try:
        logging.info("Starting the data transformation process...")
        query_job = bq_client.query(query, location="US") 
        query_job.result()  # Wait for the insertion to complete
        logging.info(f"Data from {TABLE_ID} transformed and inserted into {TABLE_TRANSFORMED_ID} successfully")
        upload_log_to_gcs()

    except Exception as e:
        logging.error(f"Failed to create/populate the table: {e}")
    finally:
        upload_log_to_gcs()

if __name__ == "__main__":
    trips_bq_to_trips_transformed_bq()
    
    
    