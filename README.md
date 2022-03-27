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

To run 



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

- https://cricsheet.org/
