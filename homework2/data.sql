/*
вывести количество фильмов в каждой категории, отсортировать по убыванию
*/
select category_id ,count(film_id) as cnt_film from film_category fc
group by category_id 
order by cnt_film desc;

/*
вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию
*/

select t.actor_id, a.first_name, a.last_name, sum_duration_rental  from(
select fa.actor_id, sum(f.rental_duration) as sum_duration_rental from film f
join film_actor fa on f.film_id = fa.film_id
group by fa.actor_id 
) as t
join actor a on a.actor_id = t.actor_id
order by sum_duration_rental desc
limit 10;

/*
вывести категорию фильмов, на которую потратили больше всего денег
*/

select c."name" , sum(p.amount) as sum_am from film f 
join film_category fc ON fc.film_id = f.film_id 
join category c on c.category_id  = fc.category_id 
join inventory i on i.film_id = f.film_id 
join rental r on r.inventory_id = i.inventory_id 
join payment p on p.rental_id = r.rental_id 
group by c."name" 
order by sum_am desc 
limit 1

/*
вывести названия фильмов, которых нет в inventory.
Написать запрос без использования оператора IN
*/

select f.title from film f 
left join inventory i on f.film_id = i.film_id
where i.inventory_id is null;

/*
вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
*/

select t2.first_name, t2.last_name from (
select *, rank() over(order by cnt desc) as rn from (
select  a.first_name, a.last_name, count(fc.film_id) as cnt from film_category fc
join film_actor  fa on fa.film_id = fc.film_id
join actor a on a.actor_id = fa.actor_id 
where category_id = 
(select category_id from category c where c."name" = 'Children')
group by a.first_name, a.last_name ) t1 ) t2
where rn <=3

/*
вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
Отсортировать по количеству неактивных клиентов по убыванию.
*/

select city, count(c.customer_id) filter(where c.active = 0) over(partition by city) as not_active,
count(c.customer_id) filter(where c.active = 1) over(partition by city) as active
from customer c
join address a on a.address_id = c.address_id
join city c2 on c2.city_id  = a.city_id
order by not_active desc 

/*
вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
и которые начинаются на букву “a”.
То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
*/

with t1 as (select sum(f.rental_duration) as sum_rent , c."name" from film f
join film_category fc on f.film_id = fc.film_id 
join category c on c.category_id = fc.category_id
join inventory i on i.film_id = f.film_id 
join store s on s.store_id = i.store_id
join customer c2 on c2.store_id = s.store_id
join address a on a.address_id = c2.address_id 
join city c3 on a.city_id = c3.city_id 
where c3.city like 'a%'
group by c."name"
order by sum_rent desc 
limit 1),
 t2 as( 
select sum(f.rental_duration) as sum_rent , c."name" from film f
join film_category fc on f.film_id = fc.film_id 
join category c on c.category_id = fc.category_id
join inventory i on i.film_id = f.film_id 
join store s on s.store_id = i.store_id
join customer c2 on c2.store_id = s.store_id
join address a on a.address_id = c2.address_id 
join city c3 on a.city_id = c3.city_id 
where c3.city like '%-%'
group by c."name"
order by sum_rent desc 
limit 1)
select * from t1
union 
select * from t2;
