SELECT
    date::TIMESTAMP AS date,
    transaction::text
FROM {{ source('nba_airflow', 'aws_transactions_source')}}