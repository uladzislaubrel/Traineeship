SELECT  name, count(film_id) AS cnt
FROM film_category AS f
INNER JOIN category AS c
ON c.category_id = f.category_id
GROUP BY 1
ORDER BY cnt DESC
