

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%';



ALTER TABLE dim_products
	ADD COLUMN weight_class TEXT;



UPDATE dim_products
SET weight_class = 'Light',
	weight = REPLACE (weight, 'kg', '')
WHERE CAST(REPLACE(weight, 'kg', '') AS FLOAT) < 2;

UPDATE dim_products
SET weight_class = 'Mid_Sized',
	weight = REPLACE (weight, 'kg', '')
WHERE CAST(REPLACE(weight, 'kg', '') AS FLOAT) >= 2
	AND CAST(REPLACE(weight, 'kg', '') AS FLOAT) < 40;

UPDATE dim_products
SET weight_class = 'Heavy',
	weight = REPLACE (weight, 'kg', '')
WHERE CAST(REPLACE(weight, 'kg', '') AS FLOAT) >= 40
	AND CAST(REPLACE(weight, 'kg', '') AS FLOAT) < 140;

UPDATE dim_products
SET weight_class = 'Truck_Required',
	weight = REPLACE (weight, 'kg', '')
WHERE CAST(REPLACE(weight, 'kg', '') AS FLOAT) >= 140;



UPDATE dim_products
SET weight = REPLACE(weight, 'kg', '');

-- SELECT *
-- FROM dim_products;

-- SELECT *
-- FROM dim_products
-- WHERE weight_class = 'Truck_Required';

SELECT *
FROM dim_products;

