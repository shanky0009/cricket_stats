{{ config(materialized='table') }}

select EXTRACT(YEAR from match_date) as year, 
    batter, 
    max(match_date) as md,
    sum(CAST(no_of_runs_from_bat as int64)) as no_of_runs_in_year
from {{ ref('runs_data') }}
where match_type = 'Test'
group by year, batter
