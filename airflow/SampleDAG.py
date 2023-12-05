"""
Sample DAG showing how to make an API request.

We would control how many DAGs run using Airflow
variables for task and DAG concurrency.
"""

import datetime
import requests

from airflow import models
from airflow.operators import PythonOperator
from google.cloud import storage

# run at midnight UTC. fudge 6 hours for MST
# in production use Variable.get('API_REFRESH_SCHEDULE', default_var=None)
schedule = "0 0 * * *"


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'schedule_interval': schedule,
}


def upload_data(bucket_name, source_string, destination):
  """
  Upload data to our S3 bucket

  :param bucket_name: Name of the bucket
  :param source_string: Source string in bytes
  :param destination: Output blob path
  """
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination)

  blob.upload_from_string(source_string)

  print('File uploaded to {}.'.format(destination))


def build_path():
    """
    Create the output path including Hive keys and values
    """
    dt = datetime.datetime.now()
    dt_iso = dt.isoformat(timespec="milliseconds")
    hours = dt.hour
    day = dt.day
    month = dt.month
    year = dt.year
    pth = f"destination/year={year}/month={month}/day={day}/hour={hours}/api_data_{dt_iso.replace(":", "_").replace(".", "_")}Z.json"
    return pth


def load_data():
    """
    Call an api and store in GCPS
    """
    response = requests.post("https://my_url", data={"key1": "val1"})
    # for json
    data = response.json()
    pth = build_path()
    upload_data("bucket", data.encode("utf-8"), pth)

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "API-Requests",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:
    api_task = PythonOperator(
        python_callable=load_data
    )

    # graph definition
    # task1 >> task2 >> branch_pick >> []...>> end
    api_task