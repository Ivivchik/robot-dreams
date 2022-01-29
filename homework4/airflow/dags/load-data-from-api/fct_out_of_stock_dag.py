import pendulum
import os
import sys
sys.path.append(os.path.join('/home/user/airflow/dags/load-data-from-api/'))

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from app.fct_out_of_stock import save_response


local_tz = pendulum.timezone("Europe/Moscow")

default_args = {
    'owner': 'Airflow',
    'provide_context': True,
    'start_date': datetime(2021, 12, 1, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id = 'fct-out-of-stock',
    description = 'Load data of products from api',
    default_args = default_args,
    schedule_interval='0 7 * * *',
    max_active_runs = 1
)

dag.doc_md = """
#### Input data: https://robot-dreams-de-api.herokuapp.com
#### Output data: ./fct-out-of-stock/data/execution_date/fct_out_of_stock.json
"""

load_data = PythonOperator(
    task_id = 'load_data',
    python_callable = save_response,
    dag = dag
)

load_data