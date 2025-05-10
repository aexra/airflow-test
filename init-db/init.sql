CREATE TABLE IF NOT EXISTS Cars (
	id serial PRIMARY KEY,
	mark text NOT NULL,
	model text NOT NULL,
	engine_volume numeric NOT NULL,
	year_of_manufacture integer NOT NULL,
	currency text NOT NULL,
	price numeric NOT NULL
);