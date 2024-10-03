-- SELECT MAX(LENGTH(product_code)) AS max_length
-- FROM orders_table;

ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(225)
	USING first_name::VARCHAR(225);

ALTER TABLE dim_users
	ALTER COLUMN last_name TYPE VARCHAR(225)
	USING last_name::VARCHAR(225);

ALTER TABLE dim_users
	ALTER COLUMN date_of_birth TYPE DATE
	USING date_of_birth::DATE;

-- SELECT MAX(LENGTH(country_code)) AS max_length
-- FROM dim_users;

ALTER TABLE dim_users
	ALTER COLUMN country_code TYPE VARCHAR(2)
	USING country_code::VARCHAR(2);

ALTER TABLE dim_users
	ALTER COLUMN user_uuid TYPE uuid
	USING user_uuid::uuid;

ALTER TABLE dim_users
	ALTER COLUMN join_date TYPE DATE
	USING join_date::DATE;

SELECT *
FROM dim_users;
