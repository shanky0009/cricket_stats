{{ config(materialized='table') }}

select * 
from {{ source('cric_transform','wickets') }}
limit 10
