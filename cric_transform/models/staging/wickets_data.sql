{{ config(materialized='view') }}

select match_id, 
innings, 
over_no, 
ball_no, 
batter, 
bowler, 
non_striker, 
JSON_EXTRACT(wickets, "$.kind") as wicket_kind, 
JSON_EXTRACT(wickets, "$.player_out") as player_out, 
JSON_EXTRACT(wickets, "$.fielders") as fielders, 
match_type, 
match_date
from {{ source('staging', 'wickets_data_clustered_partition') }}
