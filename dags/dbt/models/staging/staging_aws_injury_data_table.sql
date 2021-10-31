with injury_data as (
    SELECT player, team, date,
        {{dbt_utils.split_part('description', " ' - ' ", 1)}} as injury,
        {{dbt_utils.split_part('description', " ' - ' ", 2)}} as description
    FROM {{ source('nba_airflow', 'aws_injury_data_source')}}
),

injury_data2 as (
    SELECT *,
            {{dbt_utils.split_part('injury', " ' ' ", 1)}} as status,
            {{dbt_utils.split_part('injury', " ' ' ", 2)}} as injury2
    FROM injury_data
),

team_attributes as (

    SELECT team,
           team_acronym
    FROM {{ ref('staging_seed_team_attributes')}}
),

injury_counts as (
    SELECT team,
           count(*) as team_active_injuries
    FROM injury_data
    GROUP BY 1
),

final_stg_injury as (
    SELECT injury_data2.player,
           team_attributes.team_acronym,
           injury_data2.team,
           injury_data2.date,
           injury_data2.status,
           replace(replace(injury_data2.injury2, '(', ''), ')', '') as injury,
           injury_data2.description,
           injury_counts.team_active_injuries
    FROM injury_data2
    LEFT JOIN team_attributes using (team)
    LEFT JOIN injury_counts using (team)

)

SELECT * FROM final_stg_injury