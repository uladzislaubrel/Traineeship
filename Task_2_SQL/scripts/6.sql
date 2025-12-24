SELECT city,
SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) AS active,
SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) AS inactive
FROM customer AS c
INNER JOIN address USING (address_id)  
INNER JOIN city USING (city_id)
--WHERE city IN('Aurora','London')
GROUP BY 1
ORDER BY inactive DESC