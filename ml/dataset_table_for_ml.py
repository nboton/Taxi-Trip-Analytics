from google.cloud import bigquery, storage
from datetime import datetime, UTC
import io
import logging
from typing import Final

# =========================
# Configuration
# =========================

PROJECT_ID: Final = "taxi-trip-analytics-478020"

RAW_TABLE: Final = f"{PROJECT_ID}.dataset_raw_taxitrips.trips"
TRANSFORMED_TABLE: Final = f"{PROJECT_ID}.transformed_data.clean_filtered_data_trips"
ML_TABLE: Final = f"{PROJECT_ID}.ml_dataset.trips_ml_data"

BUCKET_NAME: Final = "taxi-trip-analytics-bucket"
GCS_LOG_FOLDER: Final = "ml/logs/"

BQ_LOCATION: Final = "US"


# =========================
# Logging setup
# =========================

def setup_logger() -> tuple[logging.Logger, io.StringIO]:
    log_stream = io.StringIO()

    logger = logging.getLogger("ml_table_creation")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False

    return logger, log_stream


# =========================
# GCS utilities
# =========================

def upload_log_to_gcs(
    storage_client: storage.Client,
    bucket_name: str,
    log_folder: str,
    log_stream: io.StringIO,
    logger: logging.Logger,
) -> None:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    log_filename = f"{log_folder}ml_table_log_{timestamp}.log"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())

    logger.info("Log file uploaded to GCS: %s", log_filename)


# =========================
# BigQuery logic
# =========================

def build_ml_query(transformed_table: str, ml_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{ml_table}` AS
    SELECT *
    FROM `{transformed_table}`
    WHERE tpep_pickup_datetime >= TIMESTAMP('2024-11-01')
      AND EXTRACT(YEAR FROM tpep_pickup_datetime)
          BETWEEN 2024 AND EXTRACT(YEAR FROM CURRENT_DATE())
      AND payment_type IN (1, 2)
    """


def create_ml_table(
    bq_client: bigquery.Client,
    query: str,
    logger: logging.Logger,
) -> None:
    logger.info("Starting ML table creation")

    query_job = bq_client.query(query)
    query_job.result()  # Wait for completion

    logger.info("ML table created successfully")


# =========================
# Main entry point
# =========================

def main() -> None:
    logger, log_stream = setup_logger()

    bq_client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
    storage_client = storage.Client()

    query = build_ml_query(TRANSFORMED_TABLE, ML_TABLE)

    try:
        create_ml_table(bq_client, query, logger)
    except Exception:
        logger.exception("ML table creation failed")
        raise
    finally:
        upload_log_to_gcs(
            storage_client=storage_client,
            bucket_name=BUCKET_NAME,
            log_folder=GCS_LOG_FOLDER,
            log_stream=log_stream,
            logger=logger,
        )


if __name__ == "__main__":
    main()
