--1
--Вывести количество фильмов в каждой категории, 
--отсортировать по убыванию.
SELECT ct.name,COUNT(fct.film_id) AS Count_Of_Movies
FROM category ct
JOIN film_category fct ON ct.category_id=fct.category_id
JOIN film f ON fct.film_id=f.film_id
GROUP BY ct.category_id,ct.name
ORDER BY COUNT(f.film_id) DESC

--2
--Вывести 10 актеров, чьи фильмы большего всего 
--арендовали, отсортировать по убыванию.

SELECT DISTINCT a.first_name || ' ' || a.last_name AS Actor,COUNT(re.rental_id) AS CountOfRental
FROM actor a
JOIN film_actor fa ON a.actor_id=fa.actor_id
JOIN inventory inv ON fa.film_id=inv.film_id
JOIN rental re ON inv.inventory_id=re.inventory_id
GROUP BY a.actor_id,a.first_name,a.last_name
ORDER BY CountOfRental DESC
LIMIT 10

--3
--Вывести категорию фильмов, 
--на которую потратили больше всего денег.
SELECT ct.category_id,ct.name,SUM(pa.amount) AS SpendedOnFilm
FROM category ct
JOIN film_category fct ON ct.category_id=fct.category_id
JOIN inventory inv ON fct.film_id=inv.film_id
JOIN rental re ON inv.inventory_id = re.inventory_id
JOIN payment pa ON re.rental_id=pa.rental_id
GROUP BY ct.category_id,ct.name
ORDER BY (SpendedOnFilm) DESC
LIMIT 1

--4
--Вывести названия фильмов, 
--которых нет в inventory. 
--Написать запрос без использования оператора IN.
SELECT f.title
FROM film f
EXCEPT
SELECT f.title
FROM inventory inv 
JOIN film f ON inv.film_id=f.film_id 

--5
--Вывести топ 3 актеров, которые больше 
--всего появлялись в фильмах в категории “Children”. 
--Если у нескольких актеров одинаковое кол-во фильмов, 
--вывести всех.
SELECT actor_id,Person,Counter
FROM(

	SELECT a.actor_id,a.first_name || ' ' || a.last_name AS Person,COUNT(fc.film_id) AS Counter,
	DENSE_RANK() OVER (ORDER BY COUNT(fc.film_id) DESC) AS place
	FROM actor a
	JOIN film_actor fa ON a.actor_id=fa.actor_id
    JOIN film_category fc ON fa.film_id=fc.film_id
    JOIN category ca ON fc.category_id=ca.category_id AND ca.name='Children'
	GROUP BY a.actor_id,Person
) AS TopOfActors
WHERE place<=3
ORDER BY Counter DESC

--6
--Вывести города с количеством активных и неактивных клиентов 
--(активный — customer.active = 1).
--Отсортировать по количеству неактивных клиентов по убыванию.
SELECT ci.city,COUNT(CASE WHEN cu.active = 1 THEN 1 ELSE NULL END) AS Actives,
COUNT(CASE WHEN cu.active = 0 THEN 1 ELSE NULL END) AS Pasives
FROM city ci 
JOIN address ad ON ci.city_id=ad.city_id
RIGHT JOIN customer cu ON ad.address_id=cu.address_id
GROUP BY ci.city
ORDER BY Pasives DESC

--7
--Вывести категорию фильмов, 
--у которой самое большое кол-во часов суммарной аренды в городах 
--(customer.address_id в этом city), и которые начинаются на букву “a”.
--То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

SELECT * FROM 
	(
    (
	SELECT ca.name,SUM(re.return_date-re.rental_date) AS Amount
    FROM category ca
    JOIN film_category fc ON ca.category_id = fc.category_id
    JOIN inventory inv ON fc.film_id = inv.film_id
    JOIN rental re ON inv.inventory_id = re.inventory_id
    JOIN customer cu ON re.customer_id = cu.customer_id
    JOIN address ad ON cu.address_id = ad.address_id
    JOIN city ci ON ad.city_id = ci.city_id
    WHERE ci.city LIKE 'a%'
    GROUP BY ca.name
    ORDER BY Amount DESC
    LIMIT 1
	)
    UNION ALL
    (
	SELECT ca.name,SUM(re.return_date-re.rental_date) AS Amount
    FROM category ca
    JOIN film_category fc ON ca.category_id = fc.category_id
    JOIN inventory inv ON fc.film_id = inv.film_id
    JOIN rental re ON inv.inventory_id = re.inventory_id
    JOIN customer cu ON re.customer_id = cu.customer_id
    JOIN address ad ON cu.address_id = ad.address_id
    JOIN city ci ON ad.city_id = ci.city_id
    WHERE ci.city LIKE '%-%'
    GROUP BY ca.name
    ORDER BY Amount DESC
    LIMIT 1
	)
) AS result;
