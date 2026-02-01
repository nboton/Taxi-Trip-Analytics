from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.sensors.time_delta import TimeDeltaSensor

# Define DAG default arguments
default_args = {
    'owner': 'Olive BOTON',
    'depends_on_past':False, #Controls whether a task execution depends on the success of the previous run.
    'start_date': days_ago(0), # run immediately after deployement
    'retries': 2, #Number of retry attempts if a task fails
    'retry_delay': timedelta(minutes=5), #
    
}
# Define the schedule: The job will effectively runs only on the last monday of the month at 02:00 PM
schedule = "0 02 * * 1"  # Every monday at 2 PM(minute hour day_of_month month day_of_week)
last_monday_condition = (
    "{{ execution_date.day >= 22 and execution_date.day <= 28 and execution_date.weekday() == 4 }}"
)

# DAG definition
with DAG(
    dag_id="elt_pipeline_bash",
    default_args=default_args,
    schedule_interval=schedule,
    catchup=False,  # Avoid running past executions
    description="batch ELT pipeline for NYC_yellow_taxi",
    tags=["nyc_taxi", "bigquery", "elt"],
) as dag:

    wait_for_last_monday = TimeDeltaSensor(
        task_id="wait_for_last_monday",
        delta=timedelta(seconds=1),  # Ensures execution on last monday
        mode='poke'
    )

    download_raw_data = BashOperator(
        task_id="download_taxi_data_to_gcs",
        bash_command="""
                    gsutil cp gs://taxi-trip-analytics-bucket/Scripts/download_data_to_gcs.py /tmp/download_data_to_gcs.py &&
                    python3 /tmp/download_data_to_gcs.py
                    """,
    )

    load_raw_trips_data = BashOperator(
        task_id="load_raw_data_from_gcs_to_bq",
        bash_command="""
                    gsutil cp gs://taxi-trip-analytics-bucket/Scripts/load_data_from_gcs_to_bq.py /tmp/load_data_from_gcs_to_bq.py &&
                    python3 /tmp/load_data_from_gcs_to_bq.py
                    """,
    )

    transform_trips_data = BashOperator(
        task_id="transform_data_in_bq_table",
        bash_command="""
                    gsutil cp gs://taxi-trip-analytics-bucket/Scripts/load_transformed_bq_table.py /tmp/load_transformed_bq_table.py &&
                    python3 /tmp/load_transformed_bq_table.py
                    """,
    )


    # Task dependencies
    wait_for_last_monday >> download_raw_data >> load_raw_trips_data >> transform_trips_data