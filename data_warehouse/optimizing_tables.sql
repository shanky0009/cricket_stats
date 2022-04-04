CREATE OR REPLACE TABLE `forward-camera-339110.cricket_data.match_info` as
select * from `forward-camera-339110.cricket_data.external_match_info`;


CREATE OR REPLACE TABLE `forward-camera-339110.cricket_data.runs_data_clustered`
CLUSTER BY match_type
   AS
    select rd.*, mi.match_type, DATE(mi.dates.list[offset(0)].item) as match_date
    from `forward-camera-339110.cricket_data.external_runs_data` rd
    join `forward-camera-339110.cricket_data.external_match_info` mi
    on rd.match_id = mi.match_id;


CREATE OR REPLACE TABLE `forward-camera-339110.cricket_data.runs_data_clustered_partition`
PARTITION BY DATE_TRUNC(match_date, YEAR)
   AS
    select * from `forward-camera-339110.cricket_data.runs_data_clustered`;


CREATE OR REPLACE TABLE `forward-camera-339110.cricket_data.wickets_data_clustered`
CLUSTER BY match_type
   AS
    select rd.*, mi.match_type, DATE(mi.dates.list[offset(0)].item) as match_date
    from `forward-camera-339110.cricket_data.external_wickets_data` rd
    join `forward-camera-339110.cricket_data.external_match_info` mi
    on rd.match_id = mi.match_id;


CREATE OR REPLACE TABLE `forward-camera-339110.cricket_data.wickets_data_clustered_partition`
PARTITION BY DATE_TRUNC(match_date, YEAR)
   AS
    select * from `forward-camera-339110.cricket_data.wickets_data_clustered`;
