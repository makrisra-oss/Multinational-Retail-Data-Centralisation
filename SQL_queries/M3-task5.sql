-- ALTER TABLE dim_products
-- 	ALTER COLUMN product_price TYPE FLOAT
-- 	USING product_price::FLOAT;

-- UPDATE dim_products
-- SET weight = REPLACE (weight, 'kg', '');

-- SELECT *
-- FROM dim_products;

-- ALTER TABLE dim_products
-- 	ALTER COLUMN weight TYPE FLOAT
-- 	USING weight::FLOAT;



-- SELECT MAX(LENGTH("EAN")) AS max_length
-- FROM dim_products;

ALTER TABLE dim_products
	ALTER COLUMN "EAN" TYPE VARCHAR(17)
	USING "EAN"::VARCHAR(17);



-- SELECT MAX(LENGTH(product_code)) AS max_length
-- FROM dim_products;

ALTER TABLE dim_products
	ALTER COLUMN product_code TYPE VARCHAR(11)
	USING product_code::VARCHAR(11);



-- ALTER TABLE dim_products
-- 	ALTER COLUMN date_added TYPE DATE
-- 	USING date_added::DATE;

ALTER TABLE dim_products
	ALTER COLUMN uuid TYPE UUID
	USING uuid::UUID;


ALTER TABLE dim_products
	RENAME removed TO still_available;

UPDATE dim_products
SET still_available = 'true'
WHERE still_available NOT IN ('true', 'false', '1', '0');

UPDATE dim_products
SET still_available = FALSE
WHERE still_available = 'Removed';

UPDATE dim_products
SET still_available = True
WHERE still_available = 'Still_available';

ALTER TABLE dim_products
	ALTER COLUMN still_available TYPE BOOL
	USING still_available::BOOL;

SELECT *
FROM dim_products
WHERE still_available = 'false';



-- SELECT MAX(LENGTH(weight_class)) AS max_length
-- FROM dim_products;

ALTER TABLE dim_products
	ALTER COLUMN weight_class TYPE VARCHAR(14)
	USING weight_class::VARCHAR(14);

SELECT *
FROM dim_products;