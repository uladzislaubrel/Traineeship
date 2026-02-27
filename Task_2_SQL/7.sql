WITH timing AS (
    SELECT 
        city,
        category.name AS category,
        SUM(EXTRACT(EPOCH FROM return_date - rental_date) / 3600) AS hours
    FROM 
        public.rental
    INNER JOIN 
        customer USING(customer_id)
    INNER JOIN 
        address USING(address_id)
    INNER JOIN 
        city USING(city_id)
    INNER JOIN 
        inventory USING(inventory_id)
    INNER JOIN 
        film USING(film_id)
    INNER JOIN 
        film_category USING(film_id)
    INNER JOIN 
        category USING(category_id)
    WHERE 
       city ILIKE 'A%' OR city ILIKE '%-%'  -- Использование ILIKE для нечувствительности к регистру
    GROUP BY 
        city, category.name
),

ranking AS (
    SELECT 
        city, 
        category, 
        hours,
        RANK() OVER (PARTITION BY city ORDER BY hours DESC) AS r
    FROM 
        timing
)

SELECT 
    city, 
    category, 
    hours
FROM 
    ranking
WHERE 
    r = 1;