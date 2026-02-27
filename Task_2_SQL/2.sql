SELECT CONCAT (first_name,' ', last_name) AS actor, count(rental_id) AS c
FROM rental AS r
INNER JOIN inventory AS i
ON i.inventory_id = r.inventory_id
INNER JOIN film AS f
ON f.film_id = i.film_id
INNER JOIN film_actor AS fa
ON f.film_id = fa.film_id
INNER JOIN actor AS a
ON a.actor_id = fa.actor_id
GROUP BY 1
ORDER BY c DESC
LIMIT 10