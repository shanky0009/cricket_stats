{{ config(materialized='view') }}

select match_id, 
    innings, 
    over_no, 
    ball_no, 
    batter, 
    bowler, 
    non_striker, 
    JSON_EXTRACT(runs, "$.batter") as no_of_runs_from_bat, 
    match_type, 
    match_date 
from {{ source('staging', 'runs_data_clustered_partition') }}
