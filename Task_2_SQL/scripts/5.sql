WITH counting AS(
SELECT   actor_id,CONCAT(first_name,' ', last_name) as actor, 
COUNT(film_id) AS cnt
FROM category AS c
INNER JOIN film_category USING (category_id)
INNER JOIN film USING (film_id)
INNER JOIN film_actor USING (film_id)
INNER JOIN actor USING (actor_id)
WHERE name = 'Children'
GROUP BY 1,2
ORDER BY cnt DESC),

ranking AS(
SELECT actor, DENSE_RANK() OVER (PARTITION BY 1 ORDER BY cnt DESC) as ran
FROM counting)

SELECT actor
FROM ranking
WHERE ran < 4