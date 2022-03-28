import os
import logging
from pathlib import Path

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import zipfile
import glob
import json as js
import pandas as pd
import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

year = '{{ execution_date.strftime(\'%Y\') }}'
dataset_file_template = AIRFLOW_HOME + "/{{ execution_date.strftime(\'%Y\') }}_json.zip"
dataset_url = "https://cricsheet.org/downloads/{{ execution_date.strftime(\'%Y\') }}_json.zip"
match_info_csv_file_template = AIRFLOW_HOME + "/match_info/match_info_{{ execution_date.strftime(\'%Y\') }}.csv"
match_info_parquet_file_template = AIRFLOW_HOME + "/match_info/match_info_{{ execution_date.strftime(\'%Y\') }}.parquet"
innings_info_file_template = AIRFLOW_HOME + "/innings/{{ execution_date.strftime(\'%Y\') }}"
dataset_output_folder_template = AIRFLOW_HOME + "/{{ execution_date.strftime(\'%Y\') }}"
innings_gcloud_upload_folder = "innings/{{ execution_date.strftime(\'%Y\') }}"


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file_template.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'cricket_data')

match_info_keys_required = ['city', 'dates', 'event', 'gender', 'match_type', 'match_type_number', 'officials', 'outcome', 'player_of_match', 'season', 'team_type', 'teams', 'toss', 'venue', 'match_id']

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


def upload_folder_to_gcs(bucket, source_folder, gcloud_upload_folder):
    glob_search_path = os.path.join(source_folder, "*.parquet")
    paths = glob.glob(glob_search_path)
    print(f'******_*____________________************________{source_folder}, {len(paths)}_______________***********')
    for path in paths:
        upload_to_gcs(bucket, f"{gcloud_upload_folder}/{Path(path).name}", path)


def unzip_file(src_file, output_folder):
    if not src_file.endswith('.zip'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    with zipfile.ZipFile(src_file, 'r') as zip_ref:
        zip_ref.extractall(output_folder)


def process_match_info(source_folder, output_file):
    glob_search_path = os.path.join(source_folder, "*.json")
    json_paths = glob.glob(glob_search_path)
    print(f'******_*____________________************________{len(json_paths)}_______________***********')
    match_info_list = []
    for json_path in json_paths:
        with open(json_path, 'r') as f:
            data = js.load(f)
        match_id = Path(json_path).stem
        match_info = data['info']
        match_info['match_id'] = match_id
        if isinstance(match_info['season'], int):
            match_info['season'] = str(match_info['season'])
        if 'event' in match_info.keys():
            for k, v in match_info['event'].items():
                if not isinstance(v, str):
                    match_info['event'][k] = str(v)
        match_info_list.append({k:match_info[k] for k in match_info_keys_required if k in match_info.keys()})
    match_info_df = pd.DataFrame(match_info_list)
    # output_dir = Path(f'{path_to_local_home}/match_info/{year}')
    # output_dir.mkdir(parents=True, exist_ok=True)
    # output_file = f'match_info_{year}.parquet'
    # output_dir = str(Path(output_file).parent)
    output_dir = Path(output_file).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    match_info_df.to_parquet(output_file)


def process_deliveries_data(source_folder, output_folder):
    glob_search_path = os.path.join(source_folder, "*.json")
    json_paths = glob.glob(glob_search_path)
    print(f'******_*____________________************________{len(json_paths)}_______________***********')
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    print(output_folder)
    for json_path in json_paths:
        with open(json_path, 'r') as f:
            data = js.load(f)
        match_id = Path(json_path).stem
        for i, innings in enumerate(data['innings']):
            if 'overs' not in innings.keys():
                continue
            overs = innings['overs']
            df = pd.DataFrame(overs)
            df['match_id'] = match_id
            df['innings'] = i+1
            df.to_parquet(output_folder / f'{match_id}_{i+1}.parquet')


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2004, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="cric_data_ingestion_dag",
    schedule_interval="0 23 31 12 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {dataset_url} > {dataset_file_template}"
    )

    unzip_file_task = PythonOperator(
        task_id="unzip_file_task",
        python_callable=unzip_file,
        op_kwargs={
            "src_file": dataset_file_template,
            "output_folder": dataset_output_folder_template,
        },
    )

    process_match_info_task = PythonOperator(
        task_id="process_match_info_task",
        python_callable=process_match_info,
        op_kwargs={
            "source_folder": dataset_output_folder_template,
            "output_file": match_info_parquet_file_template
        },
    )

    # check_files_task = BashOperator(
    #     task_id="check_files_task",
    #     bash_command=f"ls {path_to_local_home}/match_info/{year}"
    # )

    upload_match_info_to_gcs_task = PythonOperator(
        task_id="upload_match_info_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"match_info/match_info_{year}.parquet",
            "local_file": match_info_parquet_file_template,
        },
    )

    process_deliveries_data_task = PythonOperator(
        task_id="process_deliveries_data_task",
        python_callable=process_deliveries_data,
        op_kwargs={
            "source_folder": dataset_output_folder_template,
            "output_folder": innings_info_file_template
        },
    )

    upload_deliveries_info_to_gcs_task = PythonOperator(
        task_id="upload_deliveries_info_to_gcs_task",
        python_callable=upload_folder_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "source_folder": innings_info_file_template,
            "gcloud_upload_folder": innings_gcloud_upload_folder
        },
    )

    download_dataset_task >> unzip_file_task >> process_match_info_task >> upload_match_info_to_gcs_task >> process_deliveries_data_task >> upload_deliveries_info_to_gcs_task
