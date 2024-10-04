-- SELECT *
-- FROM dim_store_details

ALTER TABLE dim_store_details
	ALTER COLUMN latitude TYPE FLOAT
	USING latitude::FLOAT;

UPDATE dim_store_details
SET latitude = COALESCE(lat, latitude);

ALTER TABLE dim_store_details
DROP COLUMN lat;

-- SELECT *
-- FROM dim_store_details;

-- SELECT "dim_store_details", 'public'
-- FROM "information_schema"."tables"
-- WHERE 'public' = 'public' AND table_name ILIKE '%store%';

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT
	USING longitude::FLOAT;

ALTER TABLE dim_store_details
	ALTER COLUMN locality TYPE VARCHAR(225)
	USING locality::VARCHAR(225);

-- SELECT MAX(LENGTH(store_code)) AS max_length
-- FROM dim_store_details;

ALTER TABLE dim_store_details
	ALTER COLUMN store_code TYPE VARCHAR(12)
	USING store_code::VARCHAR(12);



ALTER TABLE dim_store_details
	ALTER COLUMN staff_numbers TYPE SMALLINT
	USING staff_numbers::SMALLINT;

ALTER TABLE dim_store_details
	ALTER COLUMN opening_date TYPE DATE
	USING opening_date::DATE;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255)
USING store_type::VARCHAR,
ALTER COLUMN store_type DROP NOT NULL;

ALTER TABLE dim_store_details
	ALTER COLUMN latitude TYPE FLOAT
	USING latitude::FLOAT;

-- SELECT MAX(LENGTH(country_code)) AS max_length
-- FROM dim_store_details;

ALTER TABLE dim_store_details
	ALTER COLUMN country_code TYPE VARCHAR(2)
	USING country_code::VARCHAR(2);

ALTER TABLE dim_store_details
	ALTER COLUMN continent TYPE VARCHAR(255)
	USING continent::VARCHAR(255);

SELECT *
FROM dim_store_details;





