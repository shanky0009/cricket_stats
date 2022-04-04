{{ config(materialized='table') }}

select bowler, 
    wicket_kind, 
    count(1) as c
from {{ ref('wickets_data') }}
where wicket_kind in ("\"caught\"", "\"bowled\"", "\"lbw\"", "\"hit wicket\"", "\"stumped\"", "\"caught and bowled\"")
and match_type = 'Test'
group by bowler, wicket_kind
