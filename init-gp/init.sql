CREATE TABLE IF NOT EXISTS dim_cars (
	car_sk serial PRIMARY KEY,
	mark text NOT NULL,
	model text NOT NULL,
	engine_volume text NOT NULL,
	year_of_manufacture integer NOT NULL,
	last_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_cars (
	sk serial PRIMARY KEY,
	car_sk integer NOT NULL,
	price_foreign numeric NOT NULL,
	currency_code_foreign text NOT NULL,
	price_rub numeric NOT NULL,
	ex_rate numeric NOT NULL,
	effective_from timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	effective_to timestamp NOT NULL DEFAULT 'infinity',
	is_current boolean NOT NULL DEFAULT true,
	source_filename text NOT NULL,
	load_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_car_sk FOREIGN KEY (car_sk) REFERENCES dim_cars(car_sk)
);

CREATE OR REPLACE VIEW car_price_dynamics AS
WITH monthly_data AS (
	SELECT
		fc.car_sk as car_sk,
		price_rub,
		mark,
		model,
		year_of_manufacture,
		DATE_TRUNC('month', effective_from) AS month
	FROM fact_cars fc
	JOIN dim_cars dc ON fc.car_sk = dc.car_sk
)
SELECT
    mark,
    model,
    year_of_manufacture,
    month,
    AVG(price_rub) as avg_price,
	MIN(price_rub) as min_price,
	MAX(price_rub) as max_price,
	MAX(price_rub) - MIN(price_rub) as price_range
FROM monthly_data as md
GROUP BY car_sk, mark, model, year_of_manufacture, month
ORDER BY car_sk;