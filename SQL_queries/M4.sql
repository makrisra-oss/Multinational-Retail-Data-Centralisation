-- Task 1

SELECT dim_store_details.country_code,
       COUNT(DISTINCT dim_store_details.store_code) AS total_num_stores
FROM dim_store_details
LEFT JOIN orders_table
ON dim_store_details.store_code = orders_table.store_code
GROUP BY dim_store_details.country_code
ORDER BY total_num_stores DESC;

-- Task 2

SELECT dim_store_details.locality,
       COUNT(DISTINCT dim_store_details.store_code) AS total_num_stores
FROM dim_store_details
LEFT JOIN orders_table
ON dim_store_details.store_code = orders_table.store_code
GROUP BY dim_store_details.locality
ORDER BY total_num_stores DESC
LIMIT 7;

-- Task 3

SELECT dim_date_times.month,
	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales
FROM orders_table
FULL OUTER JOIN dim_date_times
ON orders_table.date_uuid = dim_date_times.date_uuid
FULL OUTER JOIN dim_products
ON orders_table.product_code = dim_products.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales DESC
LIMIT 6;


-- Task 4

SELECT 
    CASE 
        WHEN dim_store_details.store_type = 'Web Portal' THEN 'Online' 
        ELSE 'Offline' 
    END AS store_category,
    COUNT(orders_table.date_uuid) AS number_of_sales,
    SUM(orders_table.product_quantity) AS product_quantity_count
FROM dim_store_details
LEFT JOIN orders_table
ON dim_store_details.store_code = orders_table.store_code
GROUP BY store_category
ORDER BY number_of_sales ASC;

-- Task 5

SELECT dim_store_details.store_type,
	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales,
	ROUND((COUNT(*) / CAST((SELECT COUNT(*) FROM orders_table) AS DECIMAL) * 100), 2) AS percentage
FROM orders_table
FULL OUTER JOIN dim_products
ON orders_table.product_code = dim_products.product_code
FULL OUTER JOIN dim_store_details
ON orders_table.store_code = dim_store_details.store_code
GROUP BY dim_store_details.store_type
ORDER BY total_sales DESC;

-- Task 6

SELECT dim_date_times.month, 
       dim_date_times.year, 
       ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales
FROM orders_table
FULL OUTER JOIN dim_date_times
    ON dim_date_times.date_uuid = orders_table.date_uuid
FULL OUTER JOIN dim_products
    ON dim_products.product_code = orders_table.product_code
GROUP BY dim_date_times.month, dim_date_times.year
ORDER BY total_sales DESC
LIMIT 10;

-- Task 7

SELECT dim_store_details.country_code,
	SUM(dim_store_details.staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY dim_store_details.country_code
ORDER BY total_staff_numbers DESC;

-- Task 8

SELECT dim_store_details.store_type,
	dim_store_details.country_code,
	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales
FROM orders_table
FULL OUTER JOIN dim_store_details
ON dim_store_details.store_code = orders_table.store_code
FULL OUTER JOIN dim_products
ON dim_products.product_code = orders_table.product_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type,
	dim_store_details.country_code
ORDER BY total_sales ASC;

-- Task 9

WITH full_timestamp_cte AS (
    SELECT 
        day,
        month,
        year,
        TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || timestamp, 'YYYY-MM-DD HH24:MI:SS') AS full_timestamp
    FROM dim_date_times
),

lead_cte AS (
	SELECT
	day,
	month,
	year,
	full_timestamp,
	LEAD(full_timestamp, 1) OVER (ORDER BY full_timestamp DESC) as time_difference
FROM full_timestamp_cte
	)

SELECT
    year,
	AVG(full_timestamp - time_difference) AS time_dif

	

FROM lead_cte
GROUP BY year
ORDER BY time_dif DESC
LIMIT 5;
