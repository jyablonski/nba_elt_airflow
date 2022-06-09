from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from datetime import datetime
import json
from typing import Dict
import requests
import logging

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

# def _extract_bitcoin_price():
#     return requests.get(API).json()['bitcoin']

# def _process_data(ti):
#     response = ti.xcom_pull(task_ids='extract_bitcoin_price')
#     logging.info(response)
#     processed_data = {'usd': response['usd'], 'change': response['usd_24h_change']}
#     ti.xcom_push(key='processed_data', value=processed_data)

# def _store_data(ti):
#     data = ti.xcom_pull(task_ids='process_data', key='processed_data')
#     logging.info(f"Store: {data['usd']} with change {data['change']}")

# with DAG('task_decorator_example', schedule_interval='@daily', start_date=datetime(2021, 12, 1), catchup=False) as dag:

#     extract_bitcoin_price = PythonOperator(
#         task_id='extract_bitcoin_price',
#         python_callable=_extract_bitcoin_price
#     )

#     process_data = PythonOperator(
#         task_id='process_data',
#         python_callable=_process_data
#     )

#     store_data = PythonOperator(
#         task_id='store_data',
#         python_callable=_store_data
#     )

#     extract_bitcoin_price >> process_data >> store_data

# can use decorators to avoid havin to writeout pythonoperator for everything, and doesn't require having to
# write ti.xcom_push and ti.xcom_pull
@dag(
    "task_decorator_example",
    schedule_interval="@daily",
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=["example"],
)
def taskflow():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price() -> Dict[str, float]:
        return requests.get(API).json()["bitcoin"]

    @task(multiple_outputs=True)
    def process_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        return {"usd": response["usd"], "change": response["usd_24h_change"]}

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['usd']} with change {data['change']}")

    email_notification = EmailOperator(
        task_id="email_notification",
        to="jyablonski9@gmail.com",
        subject="dag completed",
        html_content="the dag has finished",
    )

    store_data(process_data(extract_bitcoin_price())) >> email_notification


dag = taskflow()
