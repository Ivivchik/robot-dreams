from calendar import day_abbr
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, sum, rank, when
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame


spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('test-pyspark') \
    .config('spark.jars', 'postgresql-42.3.3.jar') \
    .getOrCreate()

url = 'jdbc:postgresql://192.168.1.82:5432/homework2'

properties = {
    'user': 'pguser',
    'password': 'secret',
    'driver': 'org.postgresql.Driver'
}

def read_table_from_postgres(table_name: str) -> DataFrame:
    table = spark.read.jdbc(
        url=url,
        table=table_name,
        properties=properties
    )
    return table

df_film_category = read_table_from_postgres('film_category')
df_film = read_table_from_postgres('film')
df_film_actor = read_table_from_postgres('film_actor')
df_actor = read_table_from_postgres('actor')
df_category = read_table_from_postgres('category')
df_inventory = read_table_from_postgres('inventory')
df_rental = read_table_from_postgres('rental')
df_payment = read_table_from_postgres('payment')
df_address = read_table_from_postgres('address')
df_city = read_table_from_postgres('city')
df_customer = read_table_from_postgres('customer')
df_store = read_table_from_postgres('store')


# вывести количество фильмов в каждой категории, отсортировать по убыванию

def task1() -> DataFrame:
    df = df_film_category \
        .select(col('film_id'), col('category_id')) \
        .groupBy(col('category_id')) \
        .agg(count(col('film_id')).alias('cnt_film')) \
        .orderBy(col('cnt_film').desc())
    return df

# вывести 10 актеров, чьи фильмы большего всего арендовали,
# отсортировать по убыванию

def task2() -> DataFrame:
    df_film_actor_task2 = df_film_actor.select(col('film_id'), col('actor_id'))
    df_actor_task2 = df_actor \
        .select(col('actor_id'), col('first_name'), col('last_name'))
    stg_df = df_film \
        .select(col('film_id'), col('rental_duration')) \
        .join(df_film_actor_task2, 'film_id', 'inner') \
        .groupBy(col('actor_id')) \
        .agg(sum(col('rental_duration')).alias('sum_duration_rental'))
    df = df_actor_task2 \
        .join(stg_df, 'actor_id', 'inner') \
        .orderBy(col('sum_duration_rental').desc()) \
        .limit(10)
    return df

# вывести категорию фильмов, на которую потратили больше всего денег

def task3() -> DataFrame:
    df_film_task3 = df_film.select(col('film_id'))
    df_payment_task3 = df_payment.select(col('rental_id'), col('amount'))
    df_category_task3 = df_category.select(col('category_id'), col('name'))
    df_rental_task3 = df_rental.select(col('rental_id'), col('inventory_id'))
    df_film_category_task3 = df_film_category \
        .select(col('film_id'), col('category_id'))
    df_inventory_task3 = df_inventory \
        .select(col('film_id'), col('inventory_id'))
    df = df_film_task3 \
        .join(df_film_category_task3, 'film_id', 'inner') \
        .join(df_category_task3, 'category_id', 'inner') \
        .join(df_inventory_task3, 'film_id', 'inner') \
        .join(df_rental_task3, 'inventory_id', 'inner') \
        .join(df_payment_task3, 'rental_id', 'inner') \
        .groupBy(col('name')) \
        .agg(sum(col('amount')).alias('sum_am')) \
        .orderBy(col('sum_am').desc()) \
        .limit(1)
    return df

# вывести названия фильмов, которых нет в inventory.
# Написать запрос без использования оператора IN

def task4() -> DataFrame:
    df_film_task4 = df_film.select(col('film_id'), col('title'))
    df_inventory_task4 = df_inventory \
        .select(col('film_id'), col('inventory_id'))
    df = df_film_task4 \
        .join(df_inventory_task4, 'film_id', 'left') \
        .filter(col('inventory_id').isNull()) \
        .select(col('title'))
    return df

# вывести топ 3 актеров,
# которые больше всего появлялись в фильмах в категории “Children”.
# Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

def task5() -> DataFrame:
    window_spec_task5 = Window.orderBy(col('cnt_film').desc())
    df_film_actor_task5 = df_film_actor.select(col('film_id'), col('actor_id'))
    df_category_task5 = df_category \
        .filter(col('name') == 'Children') \
        .select(col('category_id'))
    df_film_category_task5 = df_film_category \
        .select(col('film_id'), col('category_id'))
    df_actor_task5 = df_actor \
        .select(col('actor_id'), col('first_name'), col('last_name'))
    df = df_film_category_task5 \
        .join(df_film_actor_task5, 'film_id', 'inner') \
        .join(df_actor_task5, 'actor_id', 'inner') \
        .join(df_category_task5, 'category_id', 'inner') \
        .groupBy(col('first_name'), col('last_name')) \
        .agg(count(col('film_id')).alias('cnt_film')) \
        .withColumn('rn', rank().over(window_spec_task5)) \
        .filter(col('rn') <= 3) \
        .select(col('first_name'), col('last_name'))
    return df

# вывести города с количеством активных и неактивных клиентов
# (активный — customer.active = 1).
# Отсортировать по количеству неактивных клиентов по убыванию.

def task6() -> DataFrame:
    window_spec_task6 = Window.partitionBy(col('city'))
    df_customer_task6 = df_customer \
        .select(col('customer_id'), col('address_id'), col('active'))
    df_address_task6 = df_address.select(col('city_id') ,col('address_id'))
    df_city_task6 = df_city.select(col('city_id'), col('city'))  
    df = df_customer_task6 \
        .join(df_address_task6, 'address_id', 'inner') \
        .join(df_city_task6, 'city_id', 'inner') \
        .select(col('city'), col('customer_id'), col('active')) \
        .withColumn(
            'cnt_active', when(col('active') == 1,
            count(col('customer_id')).over(window_spec_task6)).otherwise(0)
        ) \
        .withColumn(
            'cnt_non_active', when(col('active') == 0,
            count(col('customer_id')).over(window_spec_task6)).otherwise(0)
        ) \
        .orderBy(col('cnt_non_active').desc()) \
        .select(col('city'), col('cnt_active'), col('cnt_non_active'))
    return df

# вывести категорию фильмов,у которой самое большое кол-во часов
# суммарной аренды в городах (customer.address_id в этом city),
# и которые начинаются на букву “a”.
# То же самое сделать для городов в которых есть символ “-”.
# Написать все в одном запросе.

def filter_table_city_like(condition: str) -> DataFrame:
    df = df_city \
        .filter(col('city').like(condition)) \
        .select(col('city_id'))
    return df

def create_stg_df() -> DataFrame:
    df_film_task7 = df_film.select(col('film_id'), col('rental_duration'))
    df_film_category_task7 = df_film_category \
        .select(col('film_id'), col('category_id'))
    df_category_task7 = df_category.select(col('name'), col('category_id'))
    df_inventory_task7 = df_inventory.select(col('film_id'), col('store_id'))
    df_store_task7 = df_store.select(col('store_id'))
    df_customer_task7 = df_customer.select(col('store_id'), col('address_id'))
    df_address_task7 = df_address.select(col('address_id'), col('city_id'))
    df = df_film_task7 \
        .join(df_film_category_task7, 'film_id', 'inner') \
        .join(df_category_task7, 'category_id', 'inner') \
        .join(df_inventory_task7, 'film_id', 'inner') \
        .join(df_store_task7, 'store_id', 'inner') \
        .join(df_customer_task7, 'store_id', 'inner') \
        .join(df_address_task7, 'address_id', 'inner')
    return df

def create_film_category_by_city(
    stg_df: DataFrame,
    certain_city_df: DataFrame
) -> DataFrame:
    df = stg_df \
        .join(certain_city_df, 'city_id', 'inner') \
        .groupBy(col('name')) \
        .agg(sum(col('rental_duration')).alias('sum_rent')) \
        .orderBy(col('sum_rent').desc()) \
        .limit(1)
    return df

def task7() -> DataFrame:
    stg_df = create_stg_df()
    stg_df.cache()
    df_city_start_with_a = filter_table_city_like('a%')
    df_city_contains_spec_lit = filter_table_city_like('%-%')
    df_film_category_by_city_start_with_a = create_film_category_by_city(
        stg_df,
        df_city_start_with_a
    )
    df_film_category_by_city_contains_spec_lit = create_film_category_by_city(
        stg_df,
        df_city_contains_spec_lit
    )
    df = df_film_category_by_city_start_with_a \
        .union(df_film_category_by_city_contains_spec_lit)
    return df
    
