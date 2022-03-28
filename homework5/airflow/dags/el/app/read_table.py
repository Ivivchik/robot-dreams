import os

def read_table_from_db(ds, **kwargs):

    db_name = kwargs['db_name']
    table_name = kwargs['table_name']
    postgresql_hook = kwargs['postgresql_hook']
    hdfs_hook = kwargs['hdfs_hook']
    query = f"COPY (SELECT * FROM {table_name}) \
        TO STDOUT WITH HEADER CSV DELIMITER ';'"
    
    try:
        hdfs_connection = hdfs_hook.get_conn()
        pg_connection = postgresql_hook.get_conn()
        cursor = pg_connection.cursor() 
        path_to_hdfs = os.path.join(
            '/',
            'bronze',
            db_name,
            table_name,
            ds,
            f'{table_name}.csv')
        with hdfs_connection.write(
            hdfs_path=path_to_hdfs,
            overwrite=True,
        ) as csv_file: cursor.copy_expert(query, csv_file)
    except: raise Exception('Error with connection postgresql and load data')


