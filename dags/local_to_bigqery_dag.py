import os
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#load bandcamps sales and bandcamp in "data" folder 
path_to_local_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
raw_data_folder_path='/opt/airflow/raw_data'
metadata_file_path='/opt/airflow/metadata/data_of_location.json'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def upload_to_gcs(bucket, gcs_path, local_path, sensors_location, record_date):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    print(sensors_location)

    client = storage.Client()
    bucket = client.bucket(bucket)

    file_name= f"{sensors_location}-{record_date}.TXT"
    file_location = f"{local_path}/{file_name}"
    blob = bucket.blob(file_name)
    uploaded_file = False
    try:
        blob.upload_from_filename(file_location)
        print("files_uploaded: " + file_name)
        uploaded_file = True
        
    except:
        print(file_name + " doesn't exist...")

    return uploaded_file
def check_if_file_exist(table_name, **context ):
    uploaded_file=context['task_instance'].xcom_pull(f"local_{table_name}_to_gcs_task")
    if uploaded_file:
        return f'gcs_{table_name}_to_bigquery_task'
    else:
        return f'finish_{table_name}_task'

    
# def gcs_to_bigquery():

with DAG(
    dag_id="local_to_bigquery_dag",
    start_date=datetime(2022, 10, 5),
    default_args=default_args,
    schedule_interval="@daily",
    render_template_as_native_obj=True,
    catchup=True,
    max_active_runs=6,
    tags=['fyp'],
) as dag:

    with open(metadata_file_path, 'r') as fp:
        metadata = json.load(fp)
        metadata = dict(metadata)

    for table_name ,sensors_location in metadata.items():

        local_to_gcs_task = PythonOperator(
            task_id=f"local_{table_name}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "gcs_path": "raw_fyp_data",
                "local_path": raw_data_folder_path,
                "sensors_location":sensors_location,
                "record_date": "{{ execution_date.strftime(\'%d%m\') }}"
            },
        )
        check_if_file_exist_task = BranchPythonOperator(
            task_id=f'check_if_{table_name}file_exist_task',
            python_callable=check_if_file_exist,
            provide_context=True,
            op_kwargs={
                "table_name" : table_name,
            },
        )
        finish_task = DummyOperator(task_id=f'finish_{table_name}_task')

        gcs_to_bigquery_task = GCSToBigQueryOperator(
            task_id=f"gcs_{table_name}_to_bigquery_task",
            bucket= BUCKET,
            schema_fields=[{"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                            {"name": "day_of_week", "type": "STRING", "mode": "REQUIRED"},
                            {"name": "CH4_concentration", "type": "FLOAT", "mode": "NULLABLE"},
                            {"name": "H2S_concentration", "type": "FLOAT", "mode": "NULLABLE"}],
            source_objects=[f"{sensors_location}-"+"{{ execution_date.strftime(\'%d%m\') }}"+".TXT"],
            destination_project_dataset_table=f"sensor_data_raw.{table_name}"+"{{ execution_date.strftime(\'%d_%m\') }}",
        )

        all_data_gcs_to_bigquery_task = GCSToBigQueryOperator(
            task_id=f"all_data_gcs_{table_name}_to_bigquery_task",
            bucket= BUCKET,
            schema_fields=[{"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                            {"name": "day_of_week", "type": "STRING", "mode": "REQUIRED"},
                            {"name": "CH4_concentration", "type": "FLOAT", "mode": "NULLABLE"},
                            {"name": "H2S_concentration", "type": "FLOAT", "mode": "NULLABLE"}],
            source_objects=[f"{sensors_location}-"+"{{ execution_date.strftime(\'%d%m\') }}"+".TXT"],
            write_disposition='WRITE_APPEND',
            destination_project_dataset_table=f"{table_name}.{table_name}"
        )

        local_to_gcs_task >> check_if_file_exist_task

        check_if_file_exist_task >> gcs_to_bigquery_task >> all_data_gcs_to_bigquery_task
        check_if_file_exist_task >> finish_task

