-- SELECT MAX(LENGTH(product_code)) AS max_length
-- FROM orders_table;

ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE uuid
	USING date_uuid::uuid;

ALTER TABLE orders_table
	ALTER COLUMN user_uuid TYPE uuid
	USING user_uuid::uuid;

ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19)
	USING card_number::VARCHAR(19);

ALTER TABLE orders_table
	ALTER COLUMN store_code TYPE VARCHAR(12)
	USING store_code::VARCHAR(12);

ALTER TABLE orders_table
	ALTER COLUMN product_code TYPE VARCHAR(11)
	USING product_code::VARCHAR(11);

ALTER TABLE orders_table
	ALTER COLUMN product_quantity TYPE SMALLINT
	USING product_quantity::SMALLINT;

SELECT *
FROM orders_table;