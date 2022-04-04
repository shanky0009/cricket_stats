from cProfile import run
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
import pickle
import numpy as np

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

year = '{{ execution_date.strftime(\'%Y\') }}'
DATASET_FILE_TEMPLATE = AIRFLOW_HOME + "/{{ execution_date.strftime(\'%Y\') }}_json.zip"
dataset_url = "https://cricsheet.org/downloads/{{ execution_date.strftime(\'%Y\') }}_json.zip"
MATCH_INFO_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/match_info/match_info_{{ execution_date.strftime(\'%Y\') }}.parquet"
MATCH_INFO_GCS_FILE_TEMPLATE = "raw/match_info/match_info_{{ execution_date.strftime(\'%Y\') }}.parquet"
INNINGS_RUNS_PICKLE_FILE_TEMPLATE = AIRFLOW_HOME + "/innings/runs_{{ execution_date.strftime(\'%Y\') }}.p"
INNINGS_RUNS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/innings/runs_{{ execution_date.strftime(\'%Y\') }}.parquet"
INNINGS_RUNS_GCS_FILE_TEMPLATE = "raw/innings/runs_{{ execution_date.strftime(\'%Y\') }}.parquet"
INNINGS_WICKETS_PICKLE_FILE_TEMPLATE = AIRFLOW_HOME + "/innings/wickets_{{ execution_date.strftime(\'%Y\') }}.p"
INNINGS_WICKETS_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + "/innings/wickets_{{ execution_date.strftime(\'%Y\') }}.parquet"
INNINGS_WICKETS_GCS_PARQUET_FILE_TEMPLATE = "raw/innings/wickets_{{ execution_date.strftime(\'%Y\') }}.parquet"
DATASET_OUTPUT_FOLDER_TEMPLATE = AIRFLOW_HOME + "/{{ execution_date.strftime(\'%Y\') }}"
INNINGS_GCLOUD_UPLOAD_FOLDER = "innings/{{ execution_date.strftime(\'%Y\') }}"


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = DATASET_FILE_TEMPLATE.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'cricket_data')

match_info_keys_required = ['city', 'dates', 'match_type', 'officials', 'outcome', 'player_of_match', 'season', 'team_type', 'teams', 'toss', 'venue', 'match_id']


def dump_pickle(file_name, data):
    with open(file_name, 'wb') as f:
       pickle.dump(data, f)


def load_pickle(filename):
    with open(filename, 'rb') as f:
        data = pickle.load(f)
    return data


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


def process_deliveries_data(source_folder, runs_parquet_file, wickets_parquet_file):
    # print(runs_pickle_file, wickets_pickle_file)
    glob_search_path = os.path.join(source_folder, "*.json")
    json_paths = glob.glob(glob_search_path)
    runs_output_folder = Path(runs_parquet_file).parent
    runs_output_folder.mkdir(parents=True, exist_ok=True)
    wickets_output_folder = Path(wickets_parquet_file).parent
    wickets_output_folder.mkdir(parents=True, exist_ok=True)
    innings_runs = []
    innings_wickets = []
    for json_path in json_paths:
        with open(json_path, 'r') as f:
            data = js.load(f)
        match_id = Path(json_path).stem
        for i, innings in enumerate(data['innings']):
            if 'overs' not in innings.keys():
                continue
            for over in innings['overs']:
                over_no = over['over']
                for ball_no, ov in enumerate(over['deliveries'], 1):
                    ov['over_no'] = over_no
                    ov['ball_no'] = ball_no
                    ov['match_id'] = match_id
                    ov['innings'] = i
                    if 'wickets' in ov:
                        # add wickets data to innings_wickets
                        if len(ov['wickets']) > 1:
                            print("F")
                            print(ov)
                            break
                        ov['wickets'] = ov['wickets'][0]
                        innings_wickets.append(ov)
                    else:
                        innings_runs.append(ov)
    wickets_df = pd.DataFrame(innings_wickets)
    wickets_df.extras.replace('NaN', np.nan, inplace=True)
    if 'review' in wickets_df.columns:
        wickets_df.drop(['review'], axis=1, inplace=True)
    if 'replacements' in wickets_df.columns:
        wickets_df.drop(['replacements'], axis=1, inplace=True)
    # wickets_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    # wickets_df.to_parquet(wickets_parquet_file)

    runs_df = pd.DataFrame(innings_runs)
    runs_df['runs'] = runs_df['runs'].apply(js.dumps)
    if 'replacements' in runs_df.columns:
        # runs_df['replacements'] = runs_df['replacements'].apply(js.dumps)
        runs_df.replacements.replace('NaN', np.nan, inplace=True)
    # runs_df['extras'] = runs_df['extras'].apply(js.dumps)
    runs_df.extras.replace('NaN', np.nan, inplace=True)
    if 'review' in runs_df.columns:
        runs_df.drop(['review'], axis=1, inplace=True)
    # runs_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    runs_df.to_parquet(runs_parquet_file)
    dump_pickle(runs_parquet_file, innings_runs)
    dump_pickle(wickets_parquet_file, innings_wickets)


def process_wickets_data(wickets_pickle_file, output_file):
    innings_wickets = load_pickle(wickets_pickle_file)
    wickets_df = pd.DataFrame(innings_wickets)
    wickets_df['wickets'] = wickets_df['wickets'].apply(js.dumps)
    wickets_df['runs'] = wickets_df['runs'].apply(js.dumps)
    wickets_df['extras'] = wickets_df['extras'].apply(js.dumps)
    wickets_df.extras.replace('NaN', np.nan, inplace=True)
    if 'review' in wickets_df.columns:
        wickets_df.drop(['review'], axis=1, inplace=True)
    if 'replacements' in wickets_df.columns:
        wickets_df.drop(['replacements'], axis=1, inplace=True)
    # wickets_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    wickets_df.to_parquet(output_file)


def process_runs_data(runs_pickle_file, output_file):
    innings_runs = load_pickle(runs_pickle_file)
    runs_df = pd.DataFrame(innings_runs)
    runs_df['runs'] = runs_df['runs'].apply(js.dumps)
    if 'replacements' in runs_df.columns:
        # runs_df['replacements'] = runs_df['replacements'].apply(js.dumps)
        runs_df.replacements.replace('NaN', np.nan, inplace=True)
    # runs_df['extras'] = runs_df['extras'].apply(js.dumps)
    runs_df.extras.replace('NaN', np.nan, inplace=True)
    if 'review' in runs_df.columns:
        runs_df.drop(['review'], axis=1, inplace=True)
    # runs_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    runs_df.to_parquet(output_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2004, 12, 31),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="cric_data_ingestion_dag",
    schedule_interval="0 23 31 12 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {dataset_url} > {DATASET_FILE_TEMPLATE}"
    )

    unzip_file_task = PythonOperator(
        task_id="unzip_file_task",
        python_callable=unzip_file,
        op_kwargs={
            "src_file": DATASET_FILE_TEMPLATE,
            "output_folder": DATASET_OUTPUT_FOLDER_TEMPLATE,
        },
    )

    process_match_info_task = PythonOperator(
        task_id="process_match_info_task",
        python_callable=process_match_info,
        op_kwargs={
            "source_folder": DATASET_OUTPUT_FOLDER_TEMPLATE,
            "output_file": MATCH_INFO_PARQUET_FILE_TEMPLATE
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
            "object_name": MATCH_INFO_GCS_FILE_TEMPLATE,
            "local_file": MATCH_INFO_PARQUET_FILE_TEMPLATE,
        },
    )

    process_deliveries_data_task = PythonOperator(
        task_id="process_deliveries_data_task",
        python_callable=process_deliveries_data,
        op_kwargs={
            "source_folder": DATASET_OUTPUT_FOLDER_TEMPLATE,
            "runs_parquet_file": INNINGS_RUNS_PICKLE_FILE_TEMPLATE,
            "wickets_parquet_file": INNINGS_WICKETS_PICKLE_FILE_TEMPLATE
        },
    )

    process_wickets_data_task = PythonOperator(
        task_id="process_wickets_data_task",
        python_callable=process_wickets_data,
        op_kwargs={
            "wickets_pickle_file": INNINGS_WICKETS_PICKLE_FILE_TEMPLATE,
            "output_file": INNINGS_WICKETS_PARQUET_FILE_TEMPLATE
        },
    )

    upload_wickets_data_to_gcs_task = PythonOperator(
        task_id="upload_wickets_data_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": INNINGS_WICKETS_GCS_PARQUET_FILE_TEMPLATE,
            "local_file": INNINGS_WICKETS_PARQUET_FILE_TEMPLATE,
        },
    )

    process_runs_data_task = PythonOperator(
        task_id="process_runs_data_task",
        python_callable=process_runs_data,
        op_kwargs={
            "runs_pickle_file": INNINGS_RUNS_PICKLE_FILE_TEMPLATE,
            "output_file": INNINGS_RUNS_PARQUET_FILE_TEMPLATE
        },
    )

    upload_runs_data_to_gcs_task = PythonOperator(
        task_id="upload_runs_data_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": INNINGS_RUNS_GCS_FILE_TEMPLATE,
            "local_file": INNINGS_RUNS_PARQUET_FILE_TEMPLATE,
        },
    )

    download_dataset_task >> unzip_file_task >> process_match_info_task >> upload_match_info_to_gcs_task >> process_deliveries_data_task >> process_wickets_data_task >> upload_wickets_data_to_gcs_task >> process_runs_data_task >> upload_runs_data_to_gcs_task
