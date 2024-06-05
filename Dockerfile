FROM quay.io/astronomer/astro-runtime:11.4.0

ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled \
    AIRFLOW__ASTRONOMER__UPDATE_CHECK_INTERVAL=0 \
    AIRFLOW_CONN_MY_POSTGRES_CONN=postgresql://user:password@localhost:5432/mydb