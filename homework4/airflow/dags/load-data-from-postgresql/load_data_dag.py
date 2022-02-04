import pendulum
import os
import sys
sys.path.append(os.path.join('/home/user/airflow/dags/load-data-from-postgresql/'))

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.sql_sensor import SqlSensor
from app.read_table import read_table_from_db

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
    dag_id = 'postgresql_to_local',
    description = 'Load tables from postgresql',
    default_args = default_args,
    schedule_interval='0 7 * * *',
    max_active_runs = 1
)

dag.doc_md = """
#### Input data: postgresql
#### Output data: ./postgresql-data/tableName/executionDate/tableName.csv
"""

postgresql_hook = PostgresHook(postgres_conn_id='my_postgres_connection')


def check_table(table_name):
    sql_sensor = SqlSensor(
        task_id = f'sql_sensor_{table_name}',
        conn_id = 'my_postgres_connection',
        sql = f'SELECT COUNT(*) FROM {table_name}',
        poke_interval = 60,
        timeout = 60 * 5,
        dag = dag,
    )
    return sql_sensor

def load_table(table_name):
    load_data = PythonOperator(
        task_id = f'load_data_{table_name}',
        python_callable = read_table_from_db,
        op_kwargs={'table_name': table_name, 'postgresql_hook': postgresql_hook},
        dag = dag
    )
    return load_data

def check_and_load(table_name):
    res = check_table(table_name) >> load_table(table_name)
    return res

list_tables_names = ['aisles', 'clients', 'departments', 'orders', 'products']
result = [check_and_load(table_name) for table_name in list_tables_names]

result