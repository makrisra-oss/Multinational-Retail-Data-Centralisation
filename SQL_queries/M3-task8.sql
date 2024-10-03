
-- SELECT *
-- FROM dim_card_details
-- WHERE card_number IS NULL;

-- SELECT * FROM dim_products
-- WHERE product_code IS NULL;

-- DELETE FROM dim_products
-- WHERE product_code IS NULL;


-- DELETE FROM dim_card_details
-- WHERE card_number NOT IN (
--     SELECT MIN(card_number)
--     FROM dim_card_details
--     GROUP BY card_number
-- );

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid);

-- DELETE FROM dim_store_details
-- WHERE store_code IS NULL;



ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code);



-- SELECT *
-- FROM dim_products;

-- SELECT product_code
-- FROM orders_table;

-- SELECT * FROM orders_table
-- WHERE date_uuid NOT IN(SELECT dim_date_times.date_uuid FROM dim_date_times);

-- DROP TABLE dim_card_details CASCADE;

-- DROP TABLE dim_date_times CASCADE;

-- DROP TABLE dim_products CASCADE;

-- DROP TABLE dim_store_details CASCADE;

-- DROP TABLE dim_users CASCADE;

-- DROP TABLE orders_table CASCADE;

SELECT *
FROM dim_card_details;

-- SELECT *
-- FROM dim_card_details car
-- WHERE NOT EXISTS (SELECT * FROM orders_table ord
-- WHERE car.card_number = ord.card_number);