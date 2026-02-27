SELECT category 
FROM(
SELECT c.name as category, sum(amount) as sum_money
FROM payment AS p
INNER JOIN rental AS r ON p.rental_id = r.rental_id
INNER JOIN inventory AS i ON i.inventory_id = r.inventory_id
--INNER JOIN film AS f ON f.film_id = i.film_id
INNER JOIN film_category AS fc ON fc.film_id = i.film_id
INNER JOIN category AS c ON c.category_id = fc.category_id
GROUP BY 1
ORDER BY sum_money DESC
LIMIT 1)
