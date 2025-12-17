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
GCS_LOG_FOLDER = "logs/"
TABLE_ID = f"{PROJECT_ID}.dataset_raw_taxitrips.trips"
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
    #log_filename = f"{GCS_LOG_FOLDER}load_log_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
    log_filename = f"{GCS_LOG_FOLDER}load_log_gcsfile_to_bq{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream_memo.getvalue())
    logging.info(f"Log file uploaded to {log_filename}")


def get_existing_files():
    """
    Retrieve the set of source file names that have already been loaded into a BigQuery table.

    This function queries a BigQuery table to find all distinct values in the 'source_file' column.
    It is useful to avoid re-processing or re-uploading files that have already been ingested.

    Returns:
        set: A set containing the names of source files already present in the BigQuery table.
    Raises:
        google.api_core.exceptions.GoogleAPIError: If the query fails due to permissions,
        network issues, or other BigQuery API errors.
    """
    query = f"""
        SELECT DISTINCT source_file 
        FROM `{TABLE_ID}`
        WHERE source_file IS NOT NULL
    """
    query_job = bq_client.query(query, location="US") 
    return {row.source_file for row in query_job.result()}

# %%
def get_gcs_files():
    """Retrieve the list of Parquet files from GCS."""
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_FOLDER)
    return {blob.name.split('/')[-1] for blob in blobs if blob.name.endswith(".parquet")}

# %%
def load_new_files():
    """Load new files from GCS to BigQuery."""
    try:
        new_files = get_gcs_files() - get_existing_files()

        if not new_files:
            logging.info("No new files to load.")
            return

        for file in new_files:
            uri = f"gs://{BUCKET_NAME}/{GCS_FOLDER}{file}"
            logging.info(f"Loading file: {uri}")

            # 1) Load file into temporary table without forcing schema
            temp_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrites data if table exists
                autodetect=True  # Let BigQuery detect types
            )

            load_job = bq_client.load_table_from_uri(uri, TEMP_TABLE_ID, job_config=temp_job_config)
            load_job.result()  # Wait for the job to finish
            logging.info(f"Loaded file into temp table: {TEMP_TABLE_ID}")
    
            # 2) Transformation and final insertion with passenger_count conversion
            query = f"""
            INSERT INTO `{TABLE_ID}`
            SELECT 
                VendorID, 
                tpep_pickup_datetime, 
                tpep_dropoff_datetime, 
                CAST(passenger_count AS FLOAT64) AS passenger_count,
                trip_distance, 
                RatecodeID, 
                store_and_fwd_flag, 
                PULocationID, 
                DOLocationID, 
                payment_type, 
                fare_amount, 
                extra, 
                mta_tax, 
                tip_amount, 
                tolls_amount, 
                improvement_surcharge, 
                total_amount, 
                congestion_surcharge, 
                cast(airport_fee as int64) as airport_fee,
                "{file}" AS source_file
            FROM `{TEMP_TABLE_ID}`
            """

            query_job = bq_client.query(query)
            query_job.result()  # Wait for the insertion to complete
            logging.info(f"Data from {TEMP_TABLE_ID} inserted into {TABLE_ID}")

            # 3) Delete the temporary table after use
            bq_client.delete_table(TEMP_TABLE_ID, not_found_ok=True)
            logging.info(f"Deleted temp table: {TEMP_TABLE_ID}")

        # Vérification finale
        destination_table = bq_client.get_table(TABLE_ID)
        logging.info(f"Loaded {destination_table.num_rows} rows into table {TABLE_ID}.")
    except Exception as e:
        logging.error(f"Error during loading process: {str(e)}")
    finally:
        upload_log_to_gcs()

if __name__ == "__main__":
    load_new_files()
    
    
    