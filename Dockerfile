FROM quay.io/astronomer/astro-runtime:12.6.0

ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled \
    AIRFLOW__ASTRONOMER__UPDATE_CHECK_INTERVAL=0 \
    AIRFLOW_CONN_MY_POSTGRES_CONN=postgresql://user:password@localhost:5432/mydb \
    AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend \
    # "variables_lookup_pattern": "^airflow_",
    AIRFLOW__SECRETS__BACKEND_KWARGS='{"variables_prefix": "/airflow/variables", "connections_prefix": "/airflow/connections"}' \
    AIRFLOW__SECRETS__CACHE_TTL_SECONDS=1800
    # PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/operators"
