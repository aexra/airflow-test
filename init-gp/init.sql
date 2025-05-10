CREATE TABLE IF NOT EXISTS dim_cars (
	car_sk serial PRIMARY KEY,
	mark text NOT NULL,
	model text NOT NULL,
	engine_volume text NOT NULL,
	year_of_manufacture integer NOT NULL,
	last_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
)

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
)