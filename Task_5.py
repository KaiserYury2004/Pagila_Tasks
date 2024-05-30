from pyspark.sql import SparkSession
import configparser
spark = SparkSession.builder.appName("PySpark_PostgreSQL").config("spark.jars", "postgresql-42.7.3.jar").getOrCreate()
config = configparser.ConfigParser()
config.read("config.ini")
#Перекидываю креды в список
CONNECTION_PROPERTIES_INI={"url":config['PostgreSQL']['url'],"user":config['PostgreSQL']['username'],"password":config['PostgreSQL']['password'],"driver":config['PostgreSQL']['driver']}

#Здесь происходит выгрузка базы данных из демо версии PostgrSQL

actor = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "actor") \
        .load()
address = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "address") \
        .load()
category = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "category") \
        .load()
city = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "city") \
        .load()
country = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "country") \
        .load()
film = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "film") \
        .load()
customer = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "customer") \
        .load()
film_actor = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "film_actor") \
        .load()
film_category = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "film_category") \
        .load()
inventory = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "inventory") \
        .load()
language = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "language") \
        .load()
payment = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "payment") \
        .load()
rental = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "rental") \
        .load()
staff = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "staff") \
        .load()
store = spark.read \
        .format("jdbc") \
        .options(**CONNECTION_PROPERTIES_INI)\
        .option("dbtable", "store") \
        .load()

#№1.Вывести количество фильмов в каждой категории, отсортировать по убыванию.

from pyspark.sql.functions import count
result_1 = category \
.join(film_category, category.category_id == film_category.category_id) \
.join(film, film_category.film_id == film.film_id) \
.groupBy(category.category_id, category.name) \
.agg(count(film.film_id).alias("Count_Of_Movies")) \
.orderBy("Count_Of_Movies", ascending=False)
print('Кол-во фильмов в каждой категории(по убыванию):')
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
print('Топ 10 актеров,чьи фильмы больше всего арендовали(по убыванию)')
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
print('Категория фильмов,на которую потратили больше всего денег!')
result_3.show()



#№4.Вывести названия фильмов, которых нет в inventory.

result_4=film.select(film.title)\
.subtract(inventory.join(film,inventory.film_id==film.film_id).select(film.title))
print('Фильмы,которых нету в inventory:')
result_4.show()


#5.Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..

from pyspark.sql.functions import col
result_5_1 = actor.join(film_actor, actor.actor_id == film_actor.actor_id) \
    .join(film_category, film_actor.film_id == film_category.film_id) \
    .join(category, (film_category.category_id == category.category_id) & (category.name == "Children"))\
    .groupBy(actor.actor_id, actor.first_name, actor.last_name) \
    .agg(count(film_category.film_id).alias("Counter"))
result_5 = result_5_1.join(
    result_5_1.select("Counter").distinct().orderBy(col("Counter").desc()).limit(3),on="Counter"
)
print('Топ 3 актеров,которые больше всего появлялись в фильмах категории "Children" :')
result_5.orderBy(col("Counter").desc()).show()


#№6.Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

from pyspark.sql.functions import when,unix_timestamp
result_6 = city \
    .join(address, city.city_id == address.city_id) \
    .join(customer, address.address_id == customer.address_id, "right")\
    .groupBy(city.city)\
    .agg(count(when(col("active") == 1, 1)).alias("Actives"),count(when(col("active") == 0, 1)).alias("Pasives"))\
    .orderBy(col("Pasives").desc())
print('Города,отсортированные по кол-ву неактивных клиентов(по убыванию):')
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
print('Категория фильмов,у которой самое большое кол-во часов суммарной аренды в городах :\nКоторые начинаются на "a"')
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
print('Которые содержат "-"')
result_7__.show()
