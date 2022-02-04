import os


def read_table_from_db(ds, **kwargs):

    table_name = kwargs['table_name']
    postgresql_hook = kwargs['postgresql_hook']
    pg_connection = postgresql_hook.get_conn()
    cursor = pg_connection.cursor()
    path_to_output = os.path.join('.', 'postgresql-data', table_name, ds)

    os.makedirs(path_to_output, exist_ok=True)    

    query = f"COPY (SELECT * FROM {table_name}) TO STDOUT WITH HEADER CSV DELIMITER ';'"

    with open(file=os.path.join(path_to_output, f'{table_name}.csv'), mode='w') as csv_file:
        cursor.copy_expert(query, csv_file)


