import os
import psycopg2

pg_creds = {
    'host': '192.168.1.82',
    'port': '5432',
    'database': 'dshop',
    'user': 'pguser',
    'password': 'secret'
}

def read_table_from_db(ds, **kwargs):

    table_name = kwargs['table_name']
    path_to_output = os.path.join('.', 'postgresql-data', table_name, ds)
    os.makedirs(path_to_output, exist_ok=True)
    
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

    query = f"COPY (SELECT * FROM {table_name}) TO STDOUT WITH HEADER CSV DELIMITER ';'"

    with open(file=os.path.join(path_to_output, f'{table_name}.csv'), mode='w') as csv_file:
        cursor.copy_expert(query, csv_file)


