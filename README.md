<!-- ABOUT THE PROJECT -->
## About The Project

Cricket Stats extracts ball-by-ball cricket data from https://cricsheet.org/ to analyze the players.

It uses Apache Airflow to orchestrate data extraction for matches played each year(Since 2004). There is a zip file for each year which contains json files which have data for all matches played in that year.

Each json files has information which follows this format (https://cricsheet.org/format/json/). We need info and innings section for our analysis.

Json files are processed and stored in google cloud storage as parquet files, this files are used to create table in BigQuery.

DBT(data build tool) is used to transform this raw data into schema which can be easily queried and visualized.


### Built With

* [Python](https://www.python.org/)
* [Apache Airflow](https://airflow.apache.org/)
* [Data Build Tool](https://www.getdbt.com/)
* [Google Bigquery](https://cloud.google.com/bigquery)


<!-- GETTING STARTED -->
## Getting Started

To run this project locally you need to install docker, docker compose and dbt


Starting airflow

```bash
cd data_ingestion
docker compose build
docker compose up airflow-init
docker compose up
```
This will start containers required by airflow. Airflow webserver can be accessed at ...


Starting local postgres container

```bash
cd local_project
docker compose up
```

This will start postgres container which is used to store data for local development.

Installing dbt locally



1. Go to airflow webserver(..)
2. Select '' DAG and trigger it.(after it completes we will have all data in our local postgres DB)
3. Then go to `cric_transform` directory and run `dbt run` command.(this will run the models which can be used for analytics and to create dashboard)





<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

- https://cricsheet.org/
