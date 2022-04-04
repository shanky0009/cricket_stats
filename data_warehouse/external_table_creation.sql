-- creates table for storing match_info
CREATE OR REPLACE EXTERNAL TABLE `forward-camera-339110.cricket_data.external_match_info`
OPTIONS (
  format = 'parquet',
  uris = ['gs://cricket_stats_data_lake_forward-camera-339110/raw/match_info/match_info_*.parquet']
);


-- create table for storing runs data
CREATE OR REPLACE EXTERNAL TABLE `forward-camera-339110.cricket_data.external_runs_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://cricket_stats_data_lake_forward-camera-339110/raw/innings/runs_*.parquet']
);


-- create table for storing wickets data
CREATE OR REPLACE EXTERNAL TABLE `forward-camera-339110.cricket_data.external_wickets_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://cricket_stats_data_lake_forward-camera-339110/raw/innings/wickets_*.parquet']
);
