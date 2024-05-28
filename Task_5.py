from pyspark.sql import SparkSession
import configparser
spark = SparkSession \
    .builder \
    .appName("PySpark_PostgreSQL") \
    .config("spark.jars", "postgresql-42.7.3.jar") \
    .getOrCreate()
config = configparser.ConfigParser()
config.read("config.ini")
['config.ini']
#Здесь происходит выгрузка базы данных из демо версии PostgrSQL
actor = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "actor") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
address = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "address") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
category = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "category") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
city = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "city") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
country = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "country") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
film = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "film") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
customer = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "customer") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
film_actor = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "film_actor") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
film_category = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "film_category") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
inventory = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "inventory") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
language = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "language") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
payment = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "payment") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
rental = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "rental") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
staff = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "staff") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
store = spark.read \
        .format("jdbc") \
        .option("url",config['PostgreSQL']['url']) \
        .option("dbtable", "store") \
        .option("user",config['PostgreSQL']['username'] ) \
        .option("password", config['PostgreSQL']['password']) \
        .option("driver", config['PostgreSQL']['driver']) \
        .load()
#№1.Вывести количество фильмов в каждой категории, отсортировать по убыванию.

from pyspark.sql.functions import count
result_1 = category \
.join(film_category, category.category_id == film_category.category_id) \
.join(film, film_category.film_id == film.film_id) \
.groupBy(category.category_id, category.name) \
.agg(count(film.film_id).alias("Count_Of_Movies")) \
.orderBy("Count_Of_Movies", ascending=False)
result_1.show()

#№2.Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

result_2 = actor \
.join(film_actor, actor.actor_id == film_actor.actor_id) \
.join(inventory, film_actor.film_id == inventory.film_id) \
.join(rental , inventory.inventory_id==rental.inventory_id)\
.groupBy(actor.actor_id, actor.first_name,actor.last_name) \
.agg(count(rental.rental_id).alias("Count_Of_Rental")) \
.orderBy("Count_Of_Rental", ascending=False)\
.limit(10)
result_2.show()

#№3.Вывести категорию фильмов, на которую потратили больше всего денег.

from pyspark.sql.functions import sum
result_3=category \
.join(film_category,category.category_id==film_category.category_id)\
.join(inventory,film_category.film_id==inventory.film_id)\
.join(rental,inventory.inventory_id==rental.inventory_id)\
.join(payment,rental.rental_id==payment.rental_id)\
.groupBy(category.category_id,category.name)\
.agg(sum(payment.amount).alias("SpendedOnFilm"))\
.orderBy("SpendedOnFilm",descending=True)\
.limit(1)
result_3.show()

#№4.Вывести названия фильмов, которых нет в inventory.
result_4=film.select(film.title)\
.subtract(inventory.join(film,inventory.film_id==film.film_id).select(film.title))
result_4.show()

#5.Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
from pyspark.sql.functions import col, concat_ws, count, dense_rank
from pyspark.sql.window import Window
joined_df = actor.alias('a') \
    .join(film_actor.alias('fa'), col('a.actor_id') == col('fa.actor_id')) \
    .join(film_category.alias('fc'), col('fa.film_id') == col('fc.film_id')) \
    .join(category.alias('ca'), col('fc.category_id') == col('ca.category_id')) \
    .filter(col('ca.name') == 'Children')
count_df = joined_df \
    .withColumn("Person", concat_ws(" ", col("a.first_name"), col("a.last_name"))) \
    .groupBy(col("a.actor_id"), col("Person")) \
    .agg(count(col("fc.film_id")).alias("Counter"))
window_spec = Window.orderBy(col("Counter").desc())
ranked_df = count_df.withColumn("place", dense_rank().over(window_spec))
result_5 = ranked_df.filter(col("place") <= 3).orderBy(col("Counter").desc())
result_5.show()

#№6.Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

from pyspark.sql.functions import col, count, when,unix_timestamp
result_6 = city \
    .join(address, city.city_id == address.city_id) \
    .join(customer, address.address_id == customer.address_id, "right")\
    .groupBy(city.city)\
    .agg(count(when(col("active") == 1, 1)).alias("Actives"),count(when(col("active") == 0, 1)).alias("Pasives"))\
    .orderBy(col("Pasives").desc())
result_6.show()

#№7.Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. Тоже самое сделать для городов в которых есть символ “-”.

result_7_a=city \
    .join(address, city.city_id == address.city_id) \
    .join(customer, address.address_id == customer.address_id) \
    .join(rental, customer.customer_id == rental.customer_id) \
    .join(inventory,rental.inventory_id  == inventory.inventory_id) \
    .join(film_category, inventory.film_id == film_category.film_id) \
    .join(category, film_category.category_id == category.category_id) \
    .filter(city.city.startswith('a'))\
    .withColumn("rental_duration", (unix_timestamp(rental.return_date) - unix_timestamp(rental.rental_date))) \
    .groupBy(category.category_id,category.name) \
    .agg(sum("rental_duration").alias("Amount")) \
    .orderBy("Amount") \
    .limit(1)
result_7_a.show()
result_7__=city \
    .join(address, city.city_id == address.city_id) \
    .join(customer, address.address_id == customer.address_id) \
    .join(rental, customer.customer_id == rental.customer_id) \
    .join(inventory,rental.inventory_id  == inventory.inventory_id) \
    .join(film_category, inventory.film_id == film_category.film_id) \
    .join(category, film_category.category_id == category.category_id) \
    .filter(city.city.like('%-%'))\
    .withColumn("rental_duration", (unix_timestamp(rental.return_date) - unix_timestamp(rental.rental_date))) \
    .groupBy(category.category_id,category.name) \
    .agg(sum("rental_duration").alias("Amount")) \
    .orderBy(col("Amount")) \
    .limit(1)
result_7__.show()