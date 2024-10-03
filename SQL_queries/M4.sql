-- SELECT *
-- FROM orders_table;

-- SELECT orders_table.country_code
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code;

-- SELECT dim_store_details.country_code
-- FROM orders_table;

-- SELECT dim_store_details.country_code
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code;

-- SELECT dim_store_details.country_code,
-- 		COUNT(*) AS total_num_stores
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code
-- GROUP BY dim_store_details.country_code
-- ORDER BY total_num_stores DESC;

SELECT *
FROM dim_store_details;

-- SELECT dim_store_details.store_code,
-- 	dim_store_details.country_code,
-- 	COUNT(dim_store_details.country_code) AS total_num_stores
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code
-- WHERE dim_store_details.country_code IN ('DE', 'US', 'GB')
-- GROUP BY dim_store_details.store_code, dim_store_details.country_code;

-- SELECT dim_store_details.country_code,
--        COUNT(DISTINCT dim_store_details.store_code) AS total_num_stores
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code
-- GROUP BY dim_store_details.country_code
-- ORDER BY total_num_stores DESC;

-- SELECT dim_store_details.locality,
--        COUNT(DISTINCT dim_store_details.store_code) AS total_num_stores
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code
-- GROUP BY dim_store_details.locality
-- ORDER BY total_num_stores DESC;

-- SELECT dim_date_times.month,
--        COUNT(orders_table.product_quantity) AS total_sales
-- FROM dim_date_times
-- LEFT JOIN orders_table
-- ON dim_date_times.date_uuid = orders_table.date_uuid
-- GROUP BY dim_date_times.month
-- ORDER BY total_sales DESC;

-- SELECT dim_date_times.month
-- 	SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
-- FROM orders_table
-- FULL OUTER JOIN dim_date_times
-- ON orders_table.date_uuid = dim_date_times.date_uuid
-- FULL OUTER JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
-- GROUP BY dim_date_times.month
-- ORDER BY total_sales DESC;


-- ALTER TABLE dim_products
-- 	ALTER COLUMN product_price TYPE FLOAT
-- 	USING product_price::FLOAT;
	
-- SELECT dim_date_times.month,
--        SUM(orders_table.product_quantity * CAST(dim_products.product_price AS FLOAT)) AS total_sales
-- FROM orders_table
-- INNER JOIN dim_date_times
-- ON orders_table.date_uuid = dim_date_times.date_uuid
-- INNER JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
-- GROUP BY dim_date_times.month
-- ORDER BY total_sales DESC;

-- SELECT dim_date_times.month,
--        COUNT(orders_table.product_quantity) AS total_sales
-- FROM dim_date_times
-- LEFT JOIN orders_table
-- ON dim_date_times.date_uuid = orders_table.date_uuid
-- GROUP BY dim_date_times.month
-- ORDER BY total_sales DESC;

-- SELECT dim_store_details.store_type
-- 	COUNT(orders_table.uuid) AS number_of_sales,
-- 	COUNT(orders_table.product_quantity) AS product_quantity_count
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code
-- GROUP BY dim_store_details.store_type
-- ORDER BY number_of_sales ASC;

-- SELECT dim_store_details.store_type
-- 	CASE
-- 		WHEN dim_store_details.store_type = 'Web Portal' THEN 'Online'
-- 		ELSE 'Offline'
-- 	END AS category,
-- 	COUNT(orders_table.date_uuid) AS number_of_sales,
--     SUM(orders_table.product_quantity) AS product_quantity_count
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code
-- GROUP BY dim_store_details.store_type
-- ORDER BY number_of_sales ASC;

-- SELECT 
--     CASE 
--         WHEN dim_store_details.store_type = 'Web Portal' THEN 'Online' 
--         ELSE 'Offline' 
--     END AS store_category,
--     COUNT(orders_table.date_uuid) AS number_of_sales,
--     SUM(orders_table.product_quantity) AS product_quantity_count
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON dim_store_details.store_code = orders_table.store_code
-- GROUP BY store_category
-- ORDER BY number_of_sales ASC;

-- SELECT dim_date_times.month
-- 	SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
-- FROM orders_table
-- FULL OUTER JOIN dim_date_times
-- ON orders_table.date_uuid = dim_date_times.date_uuid
-- FULL OUTER JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
-- GROUP BY dim_date_times.month
-- ORDER BY total_sales DESC;

-- WITH sales_by_store AS (
-- SELECT dim_store_details.store_type,
-- 	SUM(orders_table.product_quantity * dim_products.product_price) AS total_sales
	
-- FROM orders_table
-- FULL OUTER JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
-- FULL OUTER JOIN dim_store_details
-- ON orders_table.store_code = dim_store_details.store_code
-- GROUP BY dim_store_details.store_type
-- ),

-- total_sales AS (
-- 	SELECT SUM(total_sales) AS grand_total
-- 	FROM sales_by_store
-- )
-- SELECT
-- 	sales_by_store.store_type,
-- 	sales_by_store.total_sales,
-- 	sales_by_store.total_sales / total_sales.grand_total * 100 AS sales_percentage

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

-- FROM
-- 	sales_by_store, total_sales
-- ORDER BY sales_by_store.total_sales DESC;

-- SELECT dim_date_times.month,
-- 	dim_date_times.year,
-- 	dim_products.product_price,
-- 	orders_table.product_quantity
-- 	SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales
-- FROM orders_table
-- FULL OUTER JOIN dim_date_times
-- ON dim_date_times.date_uuid = orders_table.date_uuid
-- FULL OUTER JOIN dim_products
-- ON dim_products.product_code = orders_table.product_code
-- GROUP BY dim_date_times.month
-- ORDER BY total_sales DESC;

-- SELECT dim_date_times.month, 
--        dim_date_times.year, 
--        SUM(dim_products.product_price * orders_table.product_quantity) AS total_sales
-- FROM orders_table
-- FULL OUTER JOIN dim_date_times
--     ON dim_date_times.date_uuid = orders_table.date_uuid
-- FULL OUTER JOIN dim_products
--     ON dim_products.product_code = orders_table.product_code
-- GROUP BY dim_date_times.month, dim_date_times.year
-- ORDER BY total_sales DESC;

-- SELECT dim_store_details.country_code,
--        COUNT(dim_store_details.staff_numbers) AS total_staff_numbers
-- FROM dim_store_details
-- LEFT JOIN orders_table
-- ON orders_table.store_code = dim_store_details.store_code
-- GROUP BY dim_store_details.country_code
-- ORDER BY total_staff_numbers DESC;

-- SELECT dim_store_details.country_code,
-- 	SUM(dim_store_details.staff_numbers) AS total_staff_numbers
-- FROM dim_store_details
-- GROUP BY dim_store_details.country_code
-- ORDER BY total_staff_numbers DESC;

-- Task 8

-- SELECT dim_store_details.store_type,
-- 	dim_store_details.country_code,
-- 	ROUND(CAST(SUM(orders_table.product_quantity * dim_products.product_price) AS NUMERIC), 2) AS total_sales
-- FROM orders_table
-- FULL OUTER JOIN dim_store_details
-- ON dim_store_details.store_code = orders_table.store_code
-- FULL OUTER JOIN dim_products
-- ON dim_products.product_code = orders_table.product_code
-- WHERE dim_store_details.country_code = 'DE'
-- GROUP BY dim_store_details.store_type,
-- 	dim_store_details.country_code
-- ORDER BY total_sales ASC;

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


-- SELECT *
-- FROM dim_store_details;