from asyncio import run
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
import numpy as np
from sqlalchemy import create_engine
import pickle

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

year = '{{ execution_date.strftime(\'%Y\') }}'
dataset_file_template = AIRFLOW_HOME + "/{{ execution_date.strftime(\'%Y\') }}_json.zip"
dataset_url = "https://cricsheet.org/downloads/{{ execution_date.strftime(\'%Y\') }}_json.zip"
match_info_csv_file_template = AIRFLOW_HOME + "/match_info/match_info_{{ execution_date.strftime(\'%Y\') }}.csv"
match_info_parquet_file_template = AIRFLOW_HOME + "/match_info/match_info_{{ execution_date.strftime(\'%Y\') }}.parquet"
runs_pickle_file_template = AIRFLOW_HOME + "/runs/runs_{{ execution_date.strftime(\'%Y\') }}.p"
wickets_pickle_file_template = AIRFLOW_HOME + "/wickets/wickets_{{ execution_date.strftime(\'%Y\') }}.p"
innings_info_file_template = AIRFLOW_HOME + "/innings/{{ execution_date.strftime(\'%Y\') }}"
dataset_output_folder_template = AIRFLOW_HOME + "/{{ execution_date.strftime(\'%Y\') }}"
innings_gcloud_upload_folder = "innings/{{ execution_date.strftime(\'%Y\') }}"


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file_template.replace('.csv', '.parquet')

match_info_keys_required = ['city', 'dates', 'match_type', 'officials', 'outcome', 'player_of_match', 'season', 'team_type', 'teams', 'toss', 'venue', 'id']


def dump_pickle(file_name, data):
    with open(file_name, 'wb') as f:
       pickle.dump(data, f)


def load_pickle(filename):
    with open(filename, 'rb') as f:
        data = pickle.load(f)
    return data


def unzip_file(src_file, output_folder):
    if not src_file.endswith('.zip'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    with zipfile.ZipFile(src_file, 'r') as zip_ref:
        zip_ref.extractall(output_folder)


def process_match_info(source_folder, user, password, host, port, db, table_name):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    glob_search_path = os.path.join(source_folder, "*.json")
    json_paths = glob.glob(glob_search_path)
    match_info_list = []
    for json_path in json_paths:
        with open(json_path, 'r') as f:
            data = js.load(f)
        match_id = Path(json_path).stem
        match_info = data['info']
        match_info['id'] = match_id
        if isinstance(match_info['season'], int):
            match_info['season'] = str(match_info['season'])
        if 'event' in match_info.keys():
            for k, v in match_info['event'].items():
                if not isinstance(v, str):
                    match_info['event'][k] = str(v)
        match_info_list.append({k:match_info[k] for k in match_info_keys_required if k in match_info.keys()})
    match_info_df = pd.DataFrame(match_info_list)
    match_info_df['toss'] = match_info_df['toss'].apply(js.dumps)
    match_info_df['teams'] = match_info_df['teams'].apply(js.dumps)
    match_info_df['officials'] = match_info_df['officials'].apply(js.dumps)
    match_info_df['player_of_match'] = match_info_df['player_of_match'].apply(js.dumps)
    match_info_df['outcome'] = match_info_df['outcome'].apply(js.dumps)
    match_info_df['dates'] = match_info_df['dates'].apply(js.dumps)
    match_info_df.player_of_match.replace('NaN', np.nan, inplace=True)
    match_info_df.officials.replace('NaN', np.nan, inplace=True)
    match_info_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


def process_deliveries_data(source_folder, runs_pickle_file, wickets_pickle_file):
    print(runs_pickle_file, wickets_pickle_file)
    glob_search_path = os.path.join(source_folder, "*.json")
    json_paths = glob.glob(glob_search_path)
    runs_output_folder = Path(runs_pickle_file).parent
    runs_output_folder.mkdir(parents=True, exist_ok=True)
    wickets_output_folder = Path(wickets_pickle_file).parent
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
    dump_pickle(runs_pickle_file, innings_runs)
    dump_pickle(wickets_pickle_file, innings_wickets)


def process_wickets_data(wickets_pickle_file, user, password, host, port, db, table_name):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
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
    wickets_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


def process_runs_data(runs_pickle_file, user, password, host, port, db, table_name):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    innings_runs = load_pickle(runs_pickle_file)
    runs_df = pd.DataFrame(innings_runs)
    runs_df['runs'] = runs_df['runs'].apply(js.dumps)
    if 'replacements' in runs_df.columns:
        runs_df['replacements'] = runs_df['replacements'].apply(js.dumps)
        runs_df.replacements.replace('NaN', np.nan, inplace=True)
    runs_df['extras'] = runs_df['extras'].apply(js.dumps)
    runs_df.extras.replace('NaN', np.nan, inplace=True)
    if 'review' in runs_df.columns:
        runs_df.drop(['review'], axis=1, inplace=True)
    runs_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2004, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="cric_data_ingestion_dag_local",
    schedule_interval="0 23 31 12 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
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
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "port": PG_PORT,
            "db": PG_DATABASE,
            "table_name": 'match'
        },
    )

    process_deliveries_data_task = PythonOperator(
        task_id="process_deliveries_data_task",
        python_callable=process_deliveries_data,
        op_kwargs={
            "source_folder": dataset_output_folder_template,
            "runs_pickle_file": runs_pickle_file_template,
            "wickets_pickle_file": wickets_pickle_file_template
        },
    )

    process_wickets_data_task = PythonOperator(
        task_id="process_wickets_data_task",
        python_callable=process_wickets_data,
        op_kwargs={
            "wickets_pickle_file": wickets_pickle_file_template,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "port": PG_PORT,
            "db": PG_DATABASE,
            "table_name": 'wickets'
        },
    )

    process_runs_data_task = PythonOperator(
        task_id="process_runs_data_task",
        python_callable=process_runs_data,
        op_kwargs={
            "runs_pickle_file": runs_pickle_file_template,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "port": PG_PORT,
            "db": PG_DATABASE,
            "table_name": 'runs'
        },
    )

    download_dataset_task >> unzip_file_task >> process_match_info_task >> process_deliveries_data_task >> process_wickets_data_task >> process_runs_data_task
