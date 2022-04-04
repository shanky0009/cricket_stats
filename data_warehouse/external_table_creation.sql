CREATE OR REPLACE EXTERNAL TABLE `forward-camera-339110.cricket_data.external_innings_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://cricket_stats_data_lake_forward-camera-339110/innings/2005/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2006/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2007/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2008/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2009/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2010/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2011/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2012/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2013/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2014/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2015/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2016/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2017/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2018/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2019/*.parquet', 'gs://cricket_stats_data_lake_forward-camera-339110/innings/2020/*.parquet']
);


CREATE OR REPLACE EXTERNAL TABLE `forward-camera-339110.cricket_data.external_match_info`
OPTIONS (
  format = 'parquet',
  uris = ['gs://cricket_stats_data_lake_forward-camera-339110/match_info/match_info_20*.parquet']
);

