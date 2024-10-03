-- SELECT MAX(LENGTH("month")) AS max_length
-- FROM dim_date_times;

ALTER TABLE dim_date_times
	ALTER COLUMN "month" TYPE VARCHAR(10)
	USING "month"::VARCHAR(10);

-- SELECT MAX(LENGTH("year")) AS max_length
-- FROM dim_date_times;

ALTER TABLE dim_date_times
	ALTER COLUMN "year" TYPE VARCHAR(10)
	USING "year"::VARCHAR(10);

-- SELECT MAX(LENGTH("day")) AS max_length
-- FROM dim_date_times;

ALTER TABLE dim_date_times
	ALTER COLUMN "day" TYPE VARCHAR(10)
	USING "day"::VARCHAR(10);

-- SELECT MAX(LENGTH("time_period")) AS max_length
-- FROM dim_date_times;

ALTER TABLE dim_date_times
	ALTER COLUMN "time_period" TYPE VARCHAR(10)
	USING "time_period"::VARCHAR(10);

ALTER TABLE dim_date_times
	ALTER COLUMN "date_uuid" TYPE UUID
	USING date_uuid::UUID;

SELECT *
FROM dim_date_times;