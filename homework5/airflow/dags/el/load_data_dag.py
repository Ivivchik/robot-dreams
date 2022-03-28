import os
import sys
sys.path.append(os.path.join('/home/user/airflow/dags/el/'))

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.sql_sensor import SqlSensor
from common.hdfs_hook_by_insecureCliet import HDFSHookWithInsecureClient
from app.read_table import read_table_from_db
from app.fct_out_of_stock import save_response

default_args = {
    'owner': 'Airflow',
    'provide_context': True,
    'start_date': datetime(2021, 12, 1),
    'email': [
        'airflow@example.com',
    ],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='external_data_to_hdfs_el',
    description='Load data from external sources',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    max_active_runs=1,
)

dag.doc_md = """
### Input data:

- postgresql
- api https://robot-dreams-de-api.herokuapp.com

### Output data:
#### HDFS

- /bronze/dbName/tableName/execution_date/tableName.csv
- /bronze/fct-out-of-stock/execution_date/fct_out_of_stock.json

"""

# Добавил свой hook, по следующим соображениям
# 1. Сначала подумал про безопасность подключения(ссылка и юзер)
# 2. Подумал ссылка не такая уж и закрытая и можно ее и в коде оставить
# 3. Посмотрел как реализован hdfshook в  airflow,
# там используется другая библиотека, и она не работает с python3
# часть кода из реализации HDFSHook
#  if not snakebite_loaded:
#             raise ImportError(
#                 'This HDFSHook implementation requires snakebite, but '
#                 'snakebite is not compatible with Python 3 '
#                 '(as of August 2015). Please use Python 2 if you require '
#                 'this hook  -- or help by submitting a PR!')
# 4. По лекциям надо было использовать InsecureClient
# 5. Решил все-таки информацию о url и user, прописать в connection и
# заодно потренироваться в написание кастомных классов
hdfs_hook = HDFSHookWithInsecureClient(hdfs_conn_id='hdfs')

postgresql_hook_dshop = PostgresHook(
    postgres_conn_id='my_postgres_connection',
)
postgresql_hook_dshop_bu = PostgresHook(
    postgres_conn_id='postgres_connection_to_dshop_bu',
)

def check_table(
    db_name: str,
    table_name: str,
    postgresql_connection: str
) -> SqlSensor:
    sql_sensor = SqlSensor(
        task_id=f'sql_sensor_for_{db_name}.{table_name}',
        conn_id=postgresql_connection,
        sql=f'SELECT 1 FROM {table_name} limit 1',
        poke_interval=60,
        timeout=60*5,
        dag=dag,
    )
    return sql_sensor

def load_table_from_postgres(
    db_name: str,
    table_name: str,
    postgresql_hook: PostgresHook,
    hdfs_hook: HDFSHookWithInsecureClient
) -> PythonOperator:
    load_data = PythonOperator(
        task_id=f'load_data_from_{db_name}.{table_name}',
        python_callable=read_table_from_db,
        op_kwargs={
            'db_name': db_name,
            'table_name': table_name,
            'postgresql_hook': postgresql_hook,
            'hdfs_hook': hdfs_hook,
        },
        dag=dag,
    )
    return load_data

def check_and_load(
    db_name: str,
    table_name: str,
    postgresql_connection: str,
    postgresql_hook: PostgresHook,
    hdfs_hook: HDFSHookWithInsecureClient
) -> None:
    check_stg = check_table(
        db_name,
        table_name,
        postgresql_connection
    )
    load_stg = load_table_from_postgres(
        db_name,
        table_name,
        postgresql_hook,
        hdfs_hook
    )
    return check_stg.set_downstream(load_stg)

list_tables_names_from_dshop = [
    'aisles', 'clients', 'departments',
    'orders', 'products'
]
list_tables_names_from_dshop_bu = [
    'aisles', 'clients', 'departments', 'location_areas',
    'orders', 'products', 'store_types', 'stores'
]

generate_tasks_dshop = [
    check_and_load(
        'dshop',
        table_name, 
        'my_postgres_connection', 
        postgresql_hook_dshop,
        hdfs_hook
    ) for table_name in list_tables_names_from_dshop
]
generate_tasks_dshop_bu = [
    check_and_load(
        'dshop_bu',
        table_name,
        'postgres_connection_to_dshop_bu',
        postgresql_hook_dshop_bu,
        hdfs_hook
    ) for table_name in list_tables_names_from_dshop_bu
]

load_data_from_api = PythonOperator(
    task_id='load_data_from_api',
    python_callable=save_response,
    op_kwargs={
        'hdfs_hook': hdfs_hook,
    },
    dag = dag,
)

[generate_tasks_dshop, generate_tasks_dshop_bu, load_data_from_api]