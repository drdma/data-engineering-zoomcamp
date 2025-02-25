import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# parametrise csv files to get years accordingly
URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_PQ_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="yellow_taxi_data_2019_2020",
    schedule_interval="0 6 2 * *",      # 6am, 2nd of each month
    start_date=datetime(2019, 1, 1),    # start ingesting from 2019
    end_date=datetime(2021, 1, 1),      # stop ingesting in 2020
    default_args=default_args,
    catchup=True,                       # catch up on historic data
    max_active_runs=3,                  # increase no. of parallel runs to 3
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {YELLOW_TAXI_URL_TEMPLATE} > {YELLOW_TAXI_CSV_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": YELLOW_TAXI_CSV_FILE_TEMPLATE,
            "dest_file": YELLOW_TAXI_PQ_FILE_TEMPLATE,
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": YELLOW_TAXI_GCS_PATH_TEMPLATE,
            "local_file": YELLOW_TAXI_PQ_FILE_TEMPLATE,
        },
    )

    # create bigquery table in web UI

    # clean up local disk files
    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {YELLOW_TAXI_CSV_FILE_TEMPLATE} {YELLOW_TAXI_PQ_FILE_TEMPLATE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

# airflow needs to be forced to backfill, use terminal
# docker exec -it {docker contains of web server}
# airflow dags backfill yellow_taxi_data_2019_2020 --reset-dagruns -s 2019-01-01 -e 2021-01-01
# https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html?highlight=schedule%20interval