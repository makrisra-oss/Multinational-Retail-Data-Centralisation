-- SELECT MAX(LENGTH("card_number")) AS max_length
-- FROM dim_card_details;

ALTER TABLE dim_card_details
	ALTER COLUMN "card_number" TYPE VARCHAR(19)
	USING "card_number"::VARCHAR(19);

ALTER TABLE dim_card_details
	ALTER COLUMN "expiry_date" TYPE TEXT
	USING "expiry_date"::TEXT;

-- SELECT MAX(LENGTH("expiry_date")) AS max_length
-- FROM dim_card_details;

ALTER TABLE dim_card_details
	ALTER COLUMN "expiry_date" TYPE VARCHAR(19)
	USING "expiry_date"::VARCHAR(19);

-- SELECT MAX(LENGTH("date_payment_confirmed")) AS max_length
-- FROM dim_card_details;

ALTER TABLE dim_card_details
	ALTER COLUMN "date_payment_confirmed" TYPE DATE
	USING "date_payment_confirmed"::DATE;

SELECT *
FROM dim_card_details;