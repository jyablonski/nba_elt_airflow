select *
from nba_source.aws_boxscores_source
where 
    date(created_at) >= '{{ params.start_date }}'
    and date(created_at) < '{{ params.end_date }}'
    and fga in ({{ params.ids }})