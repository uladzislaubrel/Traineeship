SELECT title 
FROM film 
WHERE NOT EXISTS (SELECT film_id FROM inventory
WHERE film.film_id = inventory.film_id)