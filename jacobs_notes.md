# old packages
airflow-provider-great-expectations==0.0.8
airflow-dbt==0.4.0

Removing these packages as of 2022-05-01 bc of dependency issues.

# helper script
script below can create theconnections without having to manually do it ??
from airflow.models.connection import Connection

c = Connection(conn_id='my_connection',
               conn_type='postgresql',
               host='localhost',
               login='postgres',
               password='postgres',
               port=5432,
               schema='mydb',
               extra={"param1": "val1"})

uri = c.get_uri()

print(uri)

# docker compose postgres example
[link](https://github.com/apache/airflow/blob/05b44099459a7e698c3df88cec1bcad145748448/scripts/ci/docker-compose/backend-postgres.yml#L23)